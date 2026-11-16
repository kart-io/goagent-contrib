#!/usr/bin/env bash
# 启动专用 pgvector 容器并运行 memory/pgvector 全部测试（含竞态检测），结束后删除容器。
set -euo pipefail
source "$(dirname "$0")/wait-for.sh"
cd "$(dirname "$0")/../memory/pgvector"
name=goagent-pgvector-test-$$
port=${PGVECTOR_TEST_PORT:-55434}
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=test -p "127.0.0.1:$port:5432" pgvector/pgvector:0.8.0-pg17 >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
wait_for "$name" "pg_isready" docker exec "$name" pg_isready -q
sleep 1
docker exec "$name" psql -q -U postgres -c 'CREATE EXTENSION vector'
export GOAGENT_PGVECTOR_DSN="postgres://postgres:test@127.0.0.1:$port/postgres?sslmode=disable"
go vet ./...
go test -race -count=1 "$@" ./...
