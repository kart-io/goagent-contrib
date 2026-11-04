#!/usr/bin/env bash
# 对仓库中的全部 Go 模块运行 golangci-lint，配置取自仓库根目录的 .golangci.yml。
set -euo pipefail
cd "$(dirname "$0")/.."
# 同仓库的 retrieval 等模块须使用本轮源码，避免混入尚未迁移的已发布旧版本。
if [[ "${GOAGENT_CONTRIB_WORKSPACE:-}" != "1" ]]; then
  exec env GOAGENT_CONTRIB_WORKSPACE=1 bash scripts/with-local-modules.sh bash "scripts/lint-all.sh" "$@"
fi
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
