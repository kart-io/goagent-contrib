#!/usr/bin/env bash
# 启动专用 Redis 容器并运行 cache/redis 全部测试（含竞态检测），结束后删除容器。
# 默认单机：一个被测主节点，另起一对独立主从提供主从地址（不挂在被测主节点下，WAIT 无从节点的用例才成立）。
# --cluster：host 网络上 6 个实例（端口 57001–57006，集群总线 47001–47006）组成 3 主 3 从的 Cluster。
set -euo pipefail
source "$(dirname "$0")/wait-for.sh"
cluster=0
if [[ "${1:-}" == "--cluster" ]]; then
  cluster=1
  shift
fi
cd "$(dirname "$0")/../cache/redis"
name=goagent-redis-test-$$
containers=()
network=
cleanup() {
  if ((${#containers[@]})); then docker rm -f "${containers[@]}" >/dev/null 2>&1 || true; fi
  if [[ -n "$network" ]]; then docker network rm "$network" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

if ((cluster)); then
  ports=(57001 57002 57003 57004 57005 57006)
  for p in "${ports[@]}"; do
    containers+=("$name-$p")
    docker run -d --rm --name "$name-$p" --network host redis:7.4 redis-server \
      --port "$p" --cluster-port "$((p - 10000))" --bind 127.0.0.1 --cluster-enabled yes \
      --cluster-config-file "nodes-$p.conf" --save '' --appendonly no >/dev/null
  done
  for p in "${ports[@]}"; do
    wait_for "$name-$p" "$p PING" bash -c "[[ \"\$(docker exec $name-$p redis-cli -p $p ping)\" == PONG ]]"
  done
  docker exec "$name-${ports[0]}" redis-cli --cluster create "${ports[@]/#/127.0.0.1:}" \
    --cluster-replicas 1 --cluster-yes >/dev/null
  replica_addr=
  for p in "${ports[@]}"; do
    wait_for "$name-$p" "$p cluster_state:ok" bash -c "docker exec $name-$p redis-cli -p $p cluster info | grep -q '^cluster_state:ok'"
    if [[ "$(docker exec "$name-$p" redis-cli -p "$p" role | head -1)" == slave ]]; then
      wait_for "$name-$p" "$p 复制连接" bash -c "docker exec $name-$p redis-cli -p $p info replication | grep -q '^master_link_status:up'"
      replica_addr=${replica_addr:-127.0.0.1:$p}
    fi
  done
  addrs=$(printf '127.0.0.1:%s,' "${ports[@]}")
  export GOAGENT_REDIS_ADDR=${addrs%,}
  export GOAGENT_REDIS_CLUSTER=1
  export GOAGENT_REDIS_REPLICA_ADDR=$replica_addr
else
  port=${REDIS_TEST_PORT:-56379}
  replica_port=${REDIS_TEST_REPLICA_PORT:-56380}
  replicated_port=${REDIS_TEST_REPLICATED_PORT:-56381}
  network=$name
  docker network create "$network" >/dev/null
  containers+=("$name" "$name-src" "$name-replica")
  docker run -d --rm --name "$name" -p "127.0.0.1:$port:6379" redis:7.4 >/dev/null
  docker run -d --rm --name "$name-src" --network "$network" -p "127.0.0.1:$replicated_port:6379" redis:7.4 >/dev/null
  docker run -d --rm --name "$name-replica" --network "$network" -p "127.0.0.1:$replica_port:6379" redis:7.4 \
    redis-server --replicaof "$name-src" 6379 >/dev/null
  wait_for "$name" "PING" bash -c "[[ \"\$(docker exec $name redis-cli ping)\" == PONG ]]"
  wait_for "$name-replica" "从节点复制连接" bash -c "docker exec $name-replica redis-cli info replication | grep -q '^master_link_status:up'"
  export GOAGENT_REDIS_ADDR="127.0.0.1:$port"
  export GOAGENT_REDIS_REPLICA_ADDR="127.0.0.1:$replica_port"
  export GOAGENT_REDIS_REPLICATED_ADDR="127.0.0.1:$replicated_port"
fi
go vet ./...
go test -race -count=1 "$@" ./...
