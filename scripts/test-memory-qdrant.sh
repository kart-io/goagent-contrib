#!/usr/bin/env bash
# 启动专用 Qdrant 容器并运行 memory/qdrant 全部测试（含竞态检测），结束后删除容器。
set -euo pipefail
source "$(dirname "$0")/wait-for.sh"
cd "$(dirname "$0")/../memory/qdrant"
name=goagent-memory-qdrant-test-$$
port=${QDRANT_TEST_PORT:-56354}
http_port=${QDRANT_TEST_HTTP_PORT:-56353}
# 存储放 tmpfs：机械盘上建集合要 9～14 秒
docker run -d --rm --name "$name" --tmpfs /qdrant/storage -p "127.0.0.1:$port:6334" -p "127.0.0.1:$http_port:6333" qdrant/qdrant:v1.20.2 >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
wait_for "$name" "Qdrant readyz" curl -fsS "http://127.0.0.1:$http_port/readyz"
export GOAGENT_QDRANT_ADDR="127.0.0.1:$port"
go vet ./...
go test -race -count=1 "$@" ./...
