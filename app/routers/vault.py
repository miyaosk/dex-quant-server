"""
密钥保险箱 — 安全存储用户交易私钥

流程:
  1. Bot 调用 POST /vault/setup-link (需 X-Token) → 生成一次性链接
  2. 用户在浏览器打开链接 → 看到安全表单
  3. 用户粘贴私钥 + 选网络 → 提交
  4. 服务器 AES-256-GCM 加密 → 存数据库
  5. Bot 调用 GET /vault/status → 确认已存
  6. 监控产生信号时 → 取密钥解密 → 签名下单

加密: AES-256-GCM, 主密钥来自环境变量 VAULT_MASTER_KEY
"""

from __future__ import annotations

import base64
import os
import uuid
from datetime import datetime, timedelta, timezone

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from loguru import logger
from pydantic import BaseModel

from app import config, database
from app.routers.auth import validate_token

router = APIRouter(prefix="/vault", tags=["vault"])

VAULT_MASTER_KEY = os.getenv("VAULT_MASTER_KEY", "")
SETUP_LINK_TTL_MINUTES = 30


def _get_master_key() -> bytes:
    if not VAULT_MASTER_KEY:
        raise HTTPException(status_code=503, detail="Vault 未配置主密钥 (VAULT_MASTER_KEY)")
    raw = VAULT_MASTER_KEY.encode()
    if len(raw) == 32:
        return raw
    if len(raw) == 44:
        return base64.b64decode(raw)
    raise HTTPException(status_code=503, detail="VAULT_MASTER_KEY 长度无效 (需要32字节或44字符base64)")


def encrypt_private_key(plaintext: str) -> tuple[str, str, str]:
    """AES-256-GCM 加密，返回 (encrypted_b64, iv_b64, tag_b64)"""
    key = _get_master_key()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    ciphertext, tag = ct[:-16], ct[-16:]
    return (
        base64.b64encode(ciphertext).decode(),
        base64.b64encode(nonce).decode(),
        base64.b64encode(tag).decode(),
    )


def decrypt_private_key(encrypted_b64: str, iv_b64: str, tag_b64: str) -> str:
    """AES-256-GCM 解密，返回明文私钥"""
    key = _get_master_key()
    aesgcm = AESGCM(key)
    nonce = base64.b64decode(iv_b64)
    ciphertext = base64.b64decode(encrypted_b64)
    tag = base64.b64decode(tag_b64)
    plaintext = aesgcm.decrypt(nonce, ciphertext + tag, None)
    return plaintext.decode("utf-8")


# ──────────────────────────────────────────
#  API: 生成一次性设置链接 (Bot 调用，需 X-Token)
# ──────────────────────────────────────────

@router.post("/setup-link")
async def create_setup_link(request: Request, record: dict = Header(None)):
    record = await validate_token(request.headers.get("x-token", ""))
    machine_code = record["machine_code"]

    token = f"vt_{uuid.uuid4().hex}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=SETUP_LINK_TTL_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
    await database.create_vault_token(token, machine_code, expires_at)

    base_url = config.PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    url = f"{base_url}/api/v1/vault/page?token={token}"

    logger.info(f"Vault setup link created | machine={machine_code[:8]}... | expires={expires_at}")
    return {
        "url": url,
        "token": token,
        "expires_in_minutes": SETUP_LINK_TTL_MINUTES,
    }


# ──────────────────────────────────────────
#  HTML 页面: 用户在浏览器中打开 (无需 X-Token)
# ──────────────────────────────────────────

@router.get("/page", response_class=HTMLResponse, include_in_schema=False)
async def vault_page(token: str = ""):
    if not token:
        return HTMLResponse("<h2>链接无效</h2>", status_code=400)

    record = await database.get_vault_token(token)
    if not record:
        return HTMLResponse(_error_page("链接无效或已过期"), status_code=400)

    if record.get("used"):
        return HTMLResponse(_error_page("此链接已使用过，请在聊天中重新获取新链接"), status_code=400)

    expires_at = record["expires_at"]
    if isinstance(expires_at, str):
        expires_at = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    elif expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if datetime.now(timezone.utc) > expires_at:
        return HTMLResponse(_error_page("链接已过期，请在聊天中重新获取新链接"), status_code=400)

    return HTMLResponse(_submit_page(token))


# ──────────────────────────────────────────
#  提交私钥 (表单 POST，无需 X-Token)
# ──────────────────────────────────────────

class VaultSubmitRequest(BaseModel):
    token: str
    private_key: str
    network: str = "mainnet"


@router.post("/submit")
async def vault_submit(req: VaultSubmitRequest):
    record = await database.get_vault_token(req.token)
    if not record:
        raise HTTPException(status_code=400, detail="链接无效")

    if record.get("used"):
        raise HTTPException(status_code=400, detail="此链接已使用")

    expires_at = record["expires_at"]
    if isinstance(expires_at, str):
        expires_at = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    elif expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if datetime.now(timezone.utc) > expires_at:
        raise HTTPException(status_code=400, detail="链接已过期")

    pk = req.private_key.strip()
    if not pk:
        raise HTTPException(status_code=400, detail="私钥不能为空")

    machine_code = record["machine_code"]
    encrypted, iv, tag = encrypt_private_key(pk)
    await database.save_vault_key(
        machine_code=machine_code,
        encrypted_key=encrypted,
        iv=iv,
        tag=tag,
        network=req.network,
    )
    await database.mark_vault_token_used(req.token)

    logger.info(f"Vault key stored | machine={machine_code[:8]}... | network={req.network}")
    return {"status": "ok", "message": "私钥已安全存储"}


# ──────────────────────────────────────────
#  查询状态 (Bot 调用，需 X-Token)
# ──────────────────────────────────────────

@router.get("/status")
async def vault_status(request: Request):
    record = await validate_token(request.headers.get("x-token", ""))
    machine_code = record["machine_code"]

    key_record = await database.get_vault_key(machine_code)
    if not key_record:
        return {"has_key": False}

    return {
        "has_key": True,
        "network": key_record.get("network", "mainnet"),
        "created_at": str(key_record.get("created_at", "")),
        "updated_at": str(key_record.get("updated_at", "")),
    }


# ──────────────────────────────────────────
#  删除密钥 (Bot 调用，需 X-Token)
# ──────────────────────────────────────────

@router.delete("/key")
async def vault_delete_key(request: Request):
    record = await validate_token(request.headers.get("x-token", ""))
    machine_code = record["machine_code"]

    await database.delete_vault_key(machine_code)
    logger.info(f"Vault key revoked | machine={machine_code[:8]}...")
    return {"status": "ok", "message": "密钥已删除"}


# ──────────────────────────────────────────
#  内部: 供 monitor 调用，获取解密后的私钥
# ──────────────────────────────────────────

async def get_decrypted_key(machine_code: str) -> str | None:
    """监控模块调用：取出并解密用户私钥。返回 None 表示未配置。"""
    record = await database.get_vault_key(machine_code)
    if not record:
        return None
    try:
        return decrypt_private_key(record["encrypted_key"], record["iv"], record["tag"])
    except Exception as e:
        logger.error(f"Vault decrypt failed | machine={machine_code[:8]}... | {e}")
        return None


async def get_vault_network(machine_code: str) -> str:
    """获取用户配置的网络 (mainnet/testnet)。"""
    record = await database.get_vault_key(machine_code)
    return record.get("network", "mainnet") if record else "mainnet"


# ──────────────────────────────────────────
#  HTML 模板
# ──────────────────────────────────────────

def _error_page(message: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vault - 错误</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         display: flex; justify-content: center; align-items: center; min-height: 100vh;
         margin: 0; background: #0f172a; color: #f1f5f9; }}
  .card {{ background: #1e293b; padding: 40px; border-radius: 16px; max-width: 420px;
           text-align: center; box-shadow: 0 25px 50px rgba(0,0,0,0.4); }}
  .icon {{ font-size: 48px; margin-bottom: 16px; }}
  h2 {{ color: #f87171; margin: 0 0 12px; }}
  p {{ color: #94a3b8; line-height: 1.6; }}
</style>
</head>
<body>
<div class="card">
  <div class="icon">⚠️</div>
  <h2>操作失败</h2>
  <p>{message}</p>
</div>
</body></html>"""


def _submit_page(token: str) -> str:
    return """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DEX Quant - 安全密钥设置</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f172a; color: #f1f5f9; min-height: 100vh;
         display: flex; justify-content: center; align-items: center; padding: 20px; }
  .card { background: #1e293b; padding: 36px; border-radius: 16px; width: 100%%;
          max-width: 480px; box-shadow: 0 25px 50px rgba(0,0,0,0.4); }
  .logo { text-align: center; margin-bottom: 24px; }
  .logo .icon { font-size: 40px; }
  .logo h1 { font-size: 20px; margin-top: 8px; color: #e2e8f0; }
  .logo p { font-size: 13px; color: #64748b; margin-top: 4px; }
  .security-note { background: #172554; border: 1px solid #1e40af; border-radius: 10px;
                   padding: 14px 16px; margin-bottom: 24px; }
  .security-note .title { color: #60a5fa; font-weight: 600; font-size: 14px; margin-bottom: 6px; }
  .security-note ul { list-style: none; padding: 0; }
  .security-note li { color: #93c5fd; font-size: 13px; padding: 3px 0; }
  .security-note li::before { content: "✓ "; color: #34d399; }
  label { display: block; color: #94a3b8; font-size: 13px; font-weight: 500;
          margin-bottom: 6px; margin-top: 16px; }
  input, select { width: 100%%; padding: 12px 14px; border: 1px solid #334155;
                  border-radius: 8px; background: #0f172a; color: #f1f5f9;
                  font-size: 14px; outline: none; transition: border 0.2s; }
  input:focus, select:focus { border-color: #3b82f6; }
  input::placeholder { color: #475569; }
  .btn { width: 100%%; padding: 14px; border: none; border-radius: 10px;
         background: linear-gradient(135deg, #3b82f6, #2563eb); color: white;
         font-size: 16px; font-weight: 600; cursor: pointer; margin-top: 24px;
         transition: transform 0.15s, box-shadow 0.15s; }
  .btn:hover { transform: translateY(-1px); box-shadow: 0 8px 20px rgba(59,130,246,0.3); }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
  .result { text-align: center; padding: 20px 0; display: none; }
  .result.ok .icon { font-size: 48px; }
  .result.ok h2 { color: #34d399; margin: 12px 0 8px; }
  .result.ok p { color: #94a3b8; font-size: 14px; }
  .result.fail h2 { color: #f87171; }
  #form-section { }
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <div class="icon">🔐</div>
    <h1>安全密钥设置</h1>
    <p>DEX Quant 交易密钥保险箱</p>
  </div>

  <div class="security-note">
    <div class="title">🛡️ 安全保障</div>
    <ul>
      <li>私钥通过 AES-256-GCM 加密存储</li>
      <li>传输全程 HTTPS 加密</li>
      <li>不会出现在聊天记录中</li>
      <li>可随时在聊天中删除密钥</li>
    </ul>
  </div>

  <div id="form-section">
    <label for="pk">Hyperliquid 钱包私钥</label>
    <input type="password" id="pk" placeholder="0x... 或直接粘贴私钥" autocomplete="off" spellcheck="false">

    <label for="net">网络</label>
    <select id="net">
      <option value="testnet">测试网 (推荐先用这个)</option>
      <option value="mainnet">主网 (真实资金)</option>
    </select>

    <button class="btn" id="submit-btn" onclick="submitKey()">确认提交</button>
  </div>

  <div class="result ok" id="result-ok">
    <div class="icon">✅</div>
    <h2>设置成功！</h2>
    <p>私钥已安全存储，你可以关闭此页面。<br>回到聊天中继续部署策略。</p>
  </div>

  <div class="result fail" id="result-fail">
    <div class="icon">❌</div>
    <h2 id="fail-msg">提交失败</h2>
    <p id="fail-detail"></p>
  </div>
</div>

<script>
async function submitKey() {
  const pk = document.getElementById('pk').value.trim();
  const net = document.getElementById('net').value;
  const btn = document.getElementById('submit-btn');

  if (!pk) { alert('请输入私钥'); return; }

  btn.disabled = true;
  btn.textContent = '提交中...';

  try {
    const resp = await fetch('/api/v1/vault/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: '""" + token + """', private_key: pk, network: net })
    });
    const data = await resp.json();

    if (resp.ok) {
      document.getElementById('form-section').style.display = 'none';
      document.getElementById('result-ok').style.display = 'block';
    } else {
      document.getElementById('form-section').style.display = 'none';
      document.getElementById('fail-msg').textContent = '提交失败';
      document.getElementById('fail-detail').textContent = data.detail || '未知错误';
      document.getElementById('result-fail').style.display = 'block';
    }
  } catch (e) {
    btn.disabled = false;
    btn.textContent = '确认提交';
    alert('网络错误: ' + e.message);
  }
}
</script>
</body></html>"""
