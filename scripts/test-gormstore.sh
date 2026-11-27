#!/usr/bin/env bash
# 启动专用 MySQL 与 Postgres 容器（数据在 tmpfs），运行 gormstore 全部测试（含竞态检测），结束后删除容器。
set -euo pipefail
cd "$(dirname "$0")/../durable/gormstore"
pg=goagent-gormstore-pg-$$
my=goagent-gormstore-my-$$
pg_port=${GORMSTORE_TEST_PG_PORT:-55436}
my_port=${GORMSTORE_TEST_MYSQL_PORT:-53307}
mysql_image=${GORMSTORE_TEST_MYSQL_IMAGE:-mysql:8.0}
docker run -d --rm --name "$pg" --tmpfs /var/lib/postgresql/data -e POSTGRES_PASSWORD=test -p "127.0.0.1:$pg_port:5432" postgres:17.6-alpine >/dev/null
docker run -d --rm --name "$my" --tmpfs /var/lib/mysql -e MYSQL_ROOT_PASSWORD=test -p "127.0.0.1:$my_port:3306" "$mysql_image" >/dev/null
trap 'docker rm -f "$pg" "$my" >/dev/null 2>&1 || true' EXIT
until docker exec "$pg" pg_isready -q 2>/dev/null; do sleep 0.2; done
# MySQL 初始化期间会重启一次，只认经 TCP 的连接成功。
until docker exec "$my" mysqladmin ping -h 127.0.0.1 -ptest --silent 2>/dev/null; do sleep 0.5; done
sleep 1
export GOAGENT_GORMSTORE_PG_DSN="postgres://postgres:test@127.0.0.1:$pg_port/postgres?sslmode=disable"
export GOAGENT_GORMSTORE_MYSQL_DSN="root:test@tcp(127.0.0.1:$my_port)/mysql"
go vet ./...
go test -race -count=1 -timeout 20m "$@" ./...
