#!/usr/bin/env bash
# 在临时工作区中使用本仓库全部模块，不修改各模块的发布依赖或用户工作区。
set -euo pipefail
if [[ $# -eq 0 ]]; then
  echo "用法: scripts/with-local-modules.sh 命令 [参数...]" >&2
  exit 2
fi
repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
workspace_dir="$(mktemp -d "${TMPDIR:-/tmp}/goagent-contrib-work.XXXXXX")"
trap 'rm -rf "$workspace_dir"' EXIT
(cd "$workspace_dir" && GOWORK=off go work init)
export GOWORK="$workspace_dir/go.work"
while IFS= read -r mod; do
  go work use "$(dirname "$mod")"
done < <(find "$repo_root" -name go.mod -not -path '*/.git/*' | sort)
# 需要验证未发布的核心改动时显式提供路径；默认使用各 go.mod 锁定的发布版本。
if [[ -n "${GOAGENT_CORE_ROOT:-}" ]]; then
  go work use "$GOAGENT_CORE_ROOT"
fi
go work edit -go=1.27.0 -toolchain=go1.27.3
"$@"
