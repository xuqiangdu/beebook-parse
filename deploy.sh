#!/usr/bin/env bash
# Run on the target host: pull the image and recreate only the app service.
set -euo pipefail

cd "$(dirname "$0")"

echo "==> [1/3] Pull latest image from Docker Hub..."
docker compose -f docker-compose.yml pull app

echo "==> [2/3] Recreate app container only..."
docker compose -f docker-compose.yml up -d --no-deps app

echo "==> [3/3] Current status:"
docker compose -f docker-compose.yml ps

echo "==> Deploy done. Tailing last 20 lines of app logs:"
docker compose -f docker-compose.yml logs --tail=20 app
