#!/usr/bin/env bash
# 同时启动专用 Redis 与 pgvector 容器，运行 integration/distributedmemory 端到端测试（含竞态检测），结束后删除容器。
# 端口默认 Redis 56389、Postgres 55444（避开 test-redis.sh 与 test-pgvector.sh 的默认值），
# 可用 DISTRIBUTED_REDIS_PORT、DISTRIBUTED_PG_PORT 覆盖。pgstore 与 pgvector 的表建在同一个库中，由测试按用例建立。
set -euo pipefail
source "$(dirname "$0")/wait-for.sh"
cd "$(dirname "$0")/../integration/distributedmemory"
name=goagent-distributed-test-$$
redis_port=${DISTRIBUTED_REDIS_PORT:-56389}
pg_port=${DISTRIBUTED_PG_PORT:-55444}
trap 'docker rm -f "$name-redis" "$name-pg" >/dev/null 2>&1 || true' EXIT
docker run -d --rm --name "$name-redis" -p "127.0.0.1:$redis_port:6379" redis:7.4 >/dev/null
docker run -d --rm --name "$name-pg" -e POSTGRES_PASSWORD=test -p "127.0.0.1:$pg_port:5432" pgvector/pgvector:0.8.0-pg17 >/dev/null
wait_for "$name-redis" "Redis PING" bash -c "[[ \"\$(docker exec $name-redis redis-cli ping)\" == PONG ]]"
wait_for "$name-pg" "pg_isready" docker exec "$name-pg" pg_isready -q
sleep 1
docker exec "$name-pg" psql -q -U postgres -c 'CREATE EXTENSION vector'
export GOAGENT_REDIS_ADDR="127.0.0.1:$redis_port"
export GOAGENT_PGVECTOR_DSN="postgres://postgres:test@127.0.0.1:$pg_port/postgres?sslmode=disable"
go vet ./...
go test -race -count=1 "$@" ./...
