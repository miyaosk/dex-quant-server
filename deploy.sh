#!/bin/bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/dex-quant-server}"
SERVICE_NAME="${SERVICE_NAME:-dex-quant}"
PORT="${PORT:-8000}"
WORKERS="${WORKERS:-2}"
REPO_URL="${REPO_URL:-}"

echo "=== 1. 安装基础依赖 ==="
if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip python3-venv git curl
elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y python3 python3-pip python3-venv git curl
else
    echo "不支持的包管理器，请手动安装 python3 / pip / venv / git / curl"
    exit 1
fi

echo "=== 2. 准备项目目录 ==="
if [ -n "$REPO_URL" ]; then
    if [ -d "$APP_DIR/.git" ]; then
        git -C "$APP_DIR" pull --ff-only
    else
        sudo mkdir -p "$(dirname "$APP_DIR")"
        sudo git clone "$REPO_URL" "$APP_DIR"
        sudo chown -R "$(whoami)":"$(whoami)" "$APP_DIR"
    fi
else
    if [ ! -d "$APP_DIR" ]; then
        echo "APP_DIR 不存在: $APP_DIR"
        echo "请先把项目上传到服务器，或通过 REPO_URL 指定仓库地址。"
        exit 1
    fi
fi

cd "$APP_DIR"

echo "=== 3. 创建虚拟环境并安装依赖 ==="
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

echo "=== 4. 初始化环境文件 ==="
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "已从 .env.example 生成 .env，请先填好真实配置后再重跑 deploy.sh。"
    else
        echo ".env.example 不存在，请手动创建 .env。"
    fi
    exit 1
fi

echo "=== 5. 安装 systemd 服务 ==="
sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null <<EOF
[Unit]
Description=DEX Quant Server
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/gunicorn app.main:app -k uvicorn.workers.UvicornWorker -w ${WORKERS} --timeout 600 -b 0.0.0.0:${PORT} --keep-alive 10 --max-requests-jitter 100
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"
sudo systemctl restart "${SERVICE_NAME}"

echo "=== 部署完成 ==="
sudo systemctl --no-pager --full status "${SERVICE_NAME}" || true
echo ""
echo "服务监听端口: ${PORT}"
echo "日志查看命令: sudo journalctl -u ${SERVICE_NAME} -f"
