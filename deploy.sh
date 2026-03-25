#!/bin/bash
set -e

APP_DIR="/opt/dex-quant-server"
REPO_URL="https://github.com/miyaosk/dex-quant-server.git"

echo "=== 1. 安装基础依赖 ==="
if command -v apt-get &> /dev/null; then
    sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv git curl
elif command -v yum &> /dev/null; then
    sudo yum install -y python3 python3-pip git curl
fi

echo "=== 2. 克隆/更新项目 ==="
if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR"
    git pull origin master
else
    sudo git clone "$REPO_URL" "$APP_DIR"
    sudo chown -R $(whoami):$(whoami) "$APP_DIR"
    cd "$APP_DIR"
fi

echo "=== 3. 创建虚拟环境 & 安装依赖 ==="
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

echo "=== 4. 配置环境变量 ==="
if [ ! -f .env ]; then
    cat > .env << 'ENVEOF'
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your-password-here
MYSQL_DB=dex_quant
ENVEOF
    echo "⚠️  .env 文件已创建，请编辑 .env 填入真实的数据库密码！"
    echo "    vi $APP_DIR/.env"
else
    echo ".env 文件已存在，跳过"
fi

echo "=== 5. 创建 systemd 服务 ==="
sudo tee /etc/systemd/system/dex-quant.service > /dev/null << SVCEOF
[Unit]
Description=DEX Quant Server
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/gunicorn app.main:app -k uvicorn.workers.UvicornWorker -w 2 --timeout 600 -b 0.0.0.0:8000 --keep-alive 10 --max-requests-jitter 100
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF

sudo systemctl daemon-reload
sudo systemctl enable dex-quant
sudo systemctl restart dex-quant

echo "=== 部署完成 ==="
echo "服务状态:"
sudo systemctl status dex-quant --no-pager
echo ""
echo "访问地址: http://$(hostname -I | awk '{print $1}'):8000"
echo "查看日志: sudo journalctl -u dex-quant -f"
