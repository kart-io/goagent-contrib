#!/usr/bin/env bash
# 启动专用 Postgres 容器并运行 pgstore 全部测试（含竞态检测与数据库重启测试），结束后删除容器。
set -euo pipefail
cd "$(dirname "$0")/../durable/pgstore"
name=goagent-pgstore-test-$$
port=${PGSTORE_TEST_PORT:-55433}
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=test -p "127.0.0.1:$port:5432" postgres:17.6-alpine >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
until docker exec "$name" pg_isready -q 2>/dev/null; do sleep 0.2; done
sleep 1
export GOAGENT_PGSTORE_DSN="postgres://postgres:test@127.0.0.1:$port/postgres?sslmode=disable"
export GOAGENT_PGSTORE_CONTAINER="$name"
go vet ./...
go test -race -count=1 "$@" ./...
