#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="${1:-dex-sandbox:latest}"

echo "══════════════════════════════════════════"
echo "  构建 Docker 沙箱镜像: $IMAGE_NAME"
echo "══════════════════════════════════════════"

cd "$REPO_ROOT"

docker build \
    -f sandbox/Dockerfile \
    -t "$IMAGE_NAME" \
    .

echo ""
echo "✅ 镜像构建完成: $IMAGE_NAME"
echo ""
echo "启用 Docker 沙箱模式:"
echo "  export SANDBOX_MODE=docker"
echo "  export SANDBOX_IMAGE=$IMAGE_NAME"
echo ""
echo "验证镜像:"
echo "  echo '{\"script_content\":\"def generate_signals(**kw):\\n  return {\\\"signals\\\":[]}\",\"mode\":\"backtest\"}' | docker run --rm -i $IMAGE_NAME"
