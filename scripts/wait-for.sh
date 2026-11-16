# 供测试脚本 source 的等待函数。
# wait_for 容器 描述 条件命令…：每 0.2 秒检查一次条件，30 秒内不成立则打印容器日志并以失败退出。
wait_for() {
  local container=$1 what=$2
  shift 2
  for _ in $(seq 150); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "等待超时：$what，容器 $container 的日志：" >&2
  docker logs --tail 50 "$container" >&2 || true
  exit 1
}
