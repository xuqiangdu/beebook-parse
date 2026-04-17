#!/usr/bin/env bash
# 在目标机器上运行:拉最新镜像 + 重启 app 服务 + 清理悬挂镜像
set -euo pipefail

cd "$(dirname "$0")"

echo "==> [1/4] Pull latest image from Docker Hub..."
docker compose pull app

echo "==> [2/4] Recreate app container with new image..."
docker compose up -d app

echo "==> [3/4] Prune dangling images..."
docker image prune -f

echo "==> [4/4] Current status:"
docker compose ps

echo "==> Deploy done. Tailing last 20 lines of app logs:"
docker compose logs --tail=20 app
