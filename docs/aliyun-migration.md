# DEX Quant 阿里云迁移说明

本文针对当前这套 `dex-skill + dex-quant-server` 架构，从现有环境迁移到阿里云 ECS。

## 1. 目标架构

推荐最稳的形态：

- 一台阿里云 ECS 部署 `dex-quant-server`
- 一套 MySQL，优先用阿里云 RDS
- 一套 Nginx 对外暴露 HTTPS 域名
- `dex-skill` 侧通过 `DEX_QUANT_SERVER_URL` 指向新域名

示意：

```text
dex-skill / Bot / 本地脚本
          |
          | HTTPS
          v
   Nginx (quant.example.com:443)
          |
          v
   gunicorn + FastAPI (:8000)
          |
          v
      MySQL / RDS
```

## 2. 迁移前要确认的点

上线前至少要准备：

- ECS 公网 IP
- 域名，建议单独给量化服务一个子域名，例如 `quant.example.com`
- MySQL 连接信息
- `ADMIN_API_KEY`
- `VAULT_MASTER_KEY`

其中 `VAULT_MASTER_KEY` 很关键。如果你要保留历史保存过的用户私钥，必须沿用旧值，不能换。

## 3. 代码改动说明

本次已经补上的迁移相关改动：

- `dex-skill/scripts/server_config.py`
  - 新增统一的服务地址解析
- `dex-skill/scripts/api_client.py`
- `dex-skill/scripts/machine_auth.py`
- `dex-skill/scripts/strategy_runner.py`
  - 支持从 `DEX_QUANT_SERVER_URL` 读取后端地址
- `dex-quant-server/app/config.py`
  - 新增 `PUBLIC_BASE_URL`
- `dex-quant-server/app/routers/vault.py`
  - Vault 安全链接优先使用 `PUBLIC_BASE_URL`
- `dex-quant-server/.env.example`
  - 新增 ECS 可用的环境模板
- `dex-quant-server/deploy.sh`
  - 改为更适合 ECS 的部署脚本

## 4. 迁移 dex-quant-server 到 ECS

### 4.1 安装基础环境

以 Ubuntu 为例：

```bash
sudo apt-get update
sudo apt-get install -y git curl nginx python3 python3-pip python3-venv
```

如果要启用 Docker 沙箱，再补：

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
```

### 4.2 上传项目

两种方式任选一种：

1. `git clone` 到服务器
2. 本地打包上传到 `/opt/dex-quant-server`

要求最终目录类似：

```bash
/opt/dex-quant-server
```

### 4.3 配置环境变量

```bash
cd /opt/dex-quant-server
cp .env.example .env
vim .env
```

至少要改这些值：

```dotenv
MYSQL_HOST=你的MySQL地址
MYSQL_PORT=3306
MYSQL_USER=你的用户名
MYSQL_PASSWORD=你的密码
MYSQL_DB=dex_quant

PUBLIC_BASE_URL=https://quant.example.com
ADMIN_API_KEY=一个强随机字符串
VAULT_MASTER_KEY=32字节密钥或44字符base64

SANDBOX_MODE=docker
SANDBOX_IMAGE=dex-sandbox:latest
```

如果你暂时没装 Docker，可以先这样跑：

```dotenv
SANDBOX_MODE=process
```

但这只是过渡方案。正式上线更建议 `docker`。

### 4.4 构建沙箱镜像

如果启用了 Docker 沙箱：

```bash
cd /opt/dex-quant-server
bash sandbox/build.sh
```

### 4.5 启动服务

```bash
cd /opt/dex-quant-server
bash deploy.sh
```

查看状态：

```bash
sudo systemctl status dex-quant --no-pager
sudo journalctl -u dex-quant -f
```

### 4.6 配置 Nginx

`/etc/nginx/conf.d/dex-quant.conf`

```nginx
server {
    listen 80;
    server_name quant.example.com;

    client_max_body_size 20m;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $host;
    }
}
```

检查并重载：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

### 4.7 配置 HTTPS

推荐二选一：

1. 阿里云负载均衡/证书服务
2. Certbot + Let's Encrypt

完成后确认：

```bash
curl https://quant.example.com/health
```

期望返回：

```json
{"status":"ok","version":"2.1.0"}
```

## 5. 数据迁移

如果你现在已有生产 MySQL 数据，需要把这些表一起迁过去：

- `dex_machine_tokens`
- `dex_strategies`
- `dex_backtest_results`
- `dex_kline_cache`
- `dex_signals`
- `dex_monitor_jobs`
- `dex_monitor_signals`
- `dex_daily_reports`
- `dex_vault_keys`
- `dex_vault_tokens`

推荐：

```bash
mysqldump -h OLD_HOST -u USER -p --databases dex_quant > dex_quant.sql
mysql -h NEW_HOST -u USER -p < dex_quant.sql
```

注意两点：

1. `VAULT_MASTER_KEY` 必须与旧环境一致，否则 `dex_vault_keys` 中已存密钥无法解密
2. 如不需要历史 K 线缓存，可只迁业务表，不迁 `dex_kline_cache`

## 6. dex-skill 如何切到阿里云

现在 skill 端已经支持环境变量配置。

在运行 skill 的环境里设置：

```bash
export DEX_QUANT_SERVER_URL=https://quant.example.com
```

如果是长期服务进程，应该把这个变量写进：

- systemd `Environment=`
- 容器 `env`
- 机器人运行脚本
- shell profile

## 7. 迁移顺序建议

建议按这个顺序做，风险更低：

1. 在阿里云 ECS 启动新 `dex-quant-server`
2. 用临时域名或 IP 验证 `/health`
3. 配好 Nginx + HTTPS
4. 迁移 MySQL 数据
5. 用本地 `DEX_QUANT_SERVER_URL` 指向新域名做回测验证
6. 验证 Vault 链接、监控任务、策略保存
7. 再把正式 Bot/skill 环境切到新域名

## 8. 验收清单

至少验这几项：

- `GET /health` 正常
- `POST /api/v1/auth/register` 正常返回 token
- skill 端能成功回测
- 策略上传与查询正常
- `/admin/login` 可登录
- `/api/v1/vault/setup-link` 生成的链接域名正确
- monitor 启动后，服务重启能自动恢复

## 9. 当前最现实的部署建议

如果你现在是第一次迁阿里云，我建议直接按下面方式做：

- `dex-quant-server` 放 ECS
- MySQL 用 RDS
- Nginx 做 HTTPS 入口
- `SANDBOX_MODE=docker`
- `DEX_QUANT_SERVER_URL` 统一指向域名

这套结构比继续依赖 Railway 一类平台更适合你这个项目的长时监控、脚本执行和数据库持久化场景。
