#!/usr/bin/env bash
# 对仓库中的全部 Go 模块运行 golangci-lint，配置取自仓库根目录的 .golangci.yml。
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)
status=0
while IFS= read -r mod; do
  dir=$(dirname "$mod")
  echo "== $dir"
  if ! (cd "$dir" && golangci-lint run --config "$root/.golangci.yml" ./...); then
    status=1
  fi
done < <(find . -name go.mod -not -path './.git/*' | sort)
exit $status
