#!/usr/bin/env bash
# 对仓库中的全部 Go 模块运行 go vet 与 go test -race；需要外部服务的用例在未配置时自动跳过。
set -euo pipefail
cd "$(dirname "$0")/.."
status=0
while IFS= read -r mod; do
  dir=$(dirname "$mod")
  echo "== $dir"
  if ! (cd "$dir" && go vet ./... && go test -race -count=1 "$@" ./...); then
    status=1
  fi
done < <(find . -name go.mod -not -path './.git/*' | sort)
exit $status
