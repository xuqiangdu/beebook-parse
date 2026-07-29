#!/usr/bin/env bash
# Build locally and deploy only beebook-app to the test server.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
REMOTE_HOST="ubuntu@13.229.64.143"
REMOTE_DIR="/home/ubuntu/beebook-parse"
COMPOSE_FILE="docker-compose.yml"
APP_CONTAINER="beebook-app"
REDIS_CONTAINER="beebook-redis"
IMAGE="anciea/beebook-app:latest"
ROLLBACK_IMAGE="beebook-app:rollback-before-local-deploy"

SSH_OPTIONS=(
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=4
)

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

remote_container_snapshot() {
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker ps --format '{{.Names}}|{{.ID}}' | sed '/^${APP_CONTAINER}|/d' | sort"
}

rollback_app() {
  echo "Deployment verification failed. Rolling back ${APP_CONTAINER}..." >&2
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "
    set -eu
    cd '$REMOTE_DIR'
    docker image inspect '$ROLLBACK_IMAGE' >/dev/null
    docker tag '$ROLLBACK_IMAGE' '$IMAGE'
    docker compose -f '$COMPOSE_FILE' up -d \
      --no-deps --pull never --force-recreate app
  "
}

for command_name in docker ssh curl gzip; do
  require_command "$command_name"
done

cd "$ROOT_DIR"

echo "==> [1/7] Preflight checks"
docker info >/dev/null
docker buildx version >/dev/null

REMOTE_SERVICES="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "
    set -eu
    cd '$REMOTE_DIR'
    test -f '$COMPOSE_FILE'
    docker compose -f '$COMPOSE_FILE' config --services | sort | paste -sd, -
  "
)"
if [ "$REMOTE_SERVICES" != "app,redis" ]; then
  echo "Unexpected remote Compose services: ${REMOTE_SERVICES}" >&2
  exit 1
fi

APP_PROJECT="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker inspect -f '{{ index .Config.Labels \"com.docker.compose.project\" }}' '$APP_CONTAINER'"
)"
APP_SERVICE="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker inspect -f '{{ index .Config.Labels \"com.docker.compose.service\" }}' '$APP_CONTAINER'"
)"
if [ "$APP_PROJECT" != "beebook-parse" ] || [ "$APP_SERVICE" != "app" ]; then
  echo "Remote app container does not belong to beebook-parse/app" >&2
  exit 1
fi

OTHER_CONTAINERS_BEFORE="$(remote_container_snapshot)"
REDIS_BEFORE="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker inspect -f '{{.Id}}|{{.State.StartedAt}}' '$REDIS_CONTAINER'"
)"

echo "==> [2/7] Preserve the current app image for rollback"
ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "
  set -eu
  CURRENT_IMAGE_ID=\$(docker inspect -f '{{.Image}}' '$APP_CONTAINER')
  docker tag \"\$CURRENT_IMAGE_ID\" '$ROLLBACK_IMAGE'
"

echo "==> [3/7] Build linux/amd64 image locally"
docker buildx build \
  --platform linux/amd64 \
  --load \
  --tag "$IMAGE" \
  .
LOCAL_IMAGE_ID="$(docker image inspect -f '{{.Id}}' "$IMAGE")"

echo "==> [4/7] Transfer image over SSH"
docker save "$IMAGE" \
  | gzip -1 \
  | ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "gunzip | docker load"

echo "==> [5/7] Recreate only beebook-app"
if ! ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "
  set -eu
  cd '$REMOTE_DIR'
  docker compose -f '$COMPOSE_FILE' up -d \
    --no-deps --pull never --force-recreate app
  for attempt in \$(seq 1 30); do
    if curl -fsS http://127.0.0.1:5555/health \
      | grep -q '\"status\":\"ok\"'; then
      exit 0
    fi
    sleep 2
  done
  exit 1
"; then
  rollback_app
  exit 1
fi

echo "==> [6/7] Verify image, API, Redis, and unrelated containers"
REMOTE_IMAGE_ID="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker inspect -f '{{.Image}}' '$APP_CONTAINER'"
)"
if [ "$REMOTE_IMAGE_ID" != "$LOCAL_IMAGE_ID" ]; then
  rollback_app
  echo "Remote app image does not match the local build" >&2
  exit 1
fi

ADMIN_HEALTH_STATUS="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:5555/api/admin/aa-keys/health"
)"
case "$ADMIN_HEALTH_STATUS" in
  200|403|503) ;;
  *)
    rollback_app
    echo "AA key-pool health endpoint is unavailable: HTTP ${ADMIN_HEALTH_STATUS}" >&2
    exit 1
    ;;
esac

REDIS_AFTER="$(
  ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" \
    "docker inspect -f '{{.Id}}|{{.State.StartedAt}}' '$REDIS_CONTAINER'"
)"
if [ "$REDIS_AFTER" != "$REDIS_BEFORE" ]; then
  echo "Redis container changed unexpectedly" >&2
  exit 1
fi

OTHER_CONTAINERS_AFTER="$(remote_container_snapshot)"
if [ "$OTHER_CONTAINERS_AFTER" != "$OTHER_CONTAINERS_BEFORE" ]; then
  echo "A non-target container changed during deployment" >&2
  exit 1
fi

echo "==> [7/7] Deployment status"
ssh "${SSH_OPTIONS[@]}" "$REMOTE_HOST" "
  cd '$REMOTE_DIR'
  docker compose -f '$COMPOSE_FILE' ps
  docker compose -f '$COMPOSE_FILE' logs --tail=20 app
"
echo "Deployment completed. Only ${APP_CONTAINER} was recreated."
