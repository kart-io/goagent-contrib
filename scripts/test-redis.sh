#!/usr/bin/env bash
# 启动专用 Redis 容器并运行 cache/redis 全部测试（含竞态检测），结束后删除容器。
set -euo pipefail
cd "$(dirname "$0")/../cache/redis"
name=goagent-redis-test-$$
port=${REDIS_TEST_PORT:-56379}
docker run -d --rm --name "$name" -p "127.0.0.1:$port:6379" redis:7.4 >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
until [[ "$(docker exec "$name" redis-cli ping 2>/dev/null)" == "PONG" ]]; do sleep 0.2; done
export GOAGENT_REDIS_ADDR="127.0.0.1:$port"
go vet ./...
go test -race -count=1 "$@" ./...
