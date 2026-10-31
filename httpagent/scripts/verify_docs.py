#!/usr/bin/env python3
"""Check local doc links and compile complete Go package snippets.

Uses the published commit pinned by the docs in a temporary module, with no
workspace or machine-local replace. Does not check remote URLs or run models.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
VERSION = "v0.0.0-20261031151618-cfb3e85df18a"


def main():
    failures = []
    packages = []
    for path in DOCS:
        source = path.read_text(encoding="utf-8")
        for index, code in enumerate(re.findall(r"```go\s*\n(.*?)```", source, re.S), 1):
            if re.search(r"^package\s+\w+\s*$", code, re.M):
                packages.append((path, index, code))
        prose = re.sub(r"```.*?```", "", source, flags=re.S)
        prose = re.sub(r"`[^`\n]*`", "", prose)
        for target in re.findall(r"\[[^\]\n]*\]\(([^)\n]+)\)", prose):
            target = target.strip("<>").split("#", 1)[0]
            if not target or re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", target):
                continue
            if not (path.parent / unquote(target)).exists():
                failures.append(f"{path.relative_to(ROOT)}: missing local target {target}")

    env = dict(os.environ, GOWORK="off")
    with tempfile.TemporaryDirectory(prefix="httpagent-docs-") as temp:
        root = Path(temp)
        (root / "go.mod").write_text(
            "module example.com/httpagent-doc-check\n\ngo 1.25.0\n\nrequire (\n"
            f" github.com/kart-io/goagent-contrib/httpagent {VERSION}\n"
            " github.com/kart-io/goagent v0.32.2\n)\n", encoding="utf-8")
        for number, (path, index, code) in enumerate(packages):
            directory = root / f"snippet{number}"
            directory.mkdir()
            (directory / "main.go").write_text(code, encoding="utf-8")
            print(f"Compile: {path.relative_to(ROOT)} Go block {index}", flush=True)
        if packages:
            for command in (["go", "mod", "tidy"], ["go", "build", "-mod=readonly", "./..."],
                            ["go", "vet", "-mod=readonly", "./..."]):
                result = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True)
                if result.returncode:
                    failures.append(f"{' '.join(command)}:\n{result.stdout}{result.stderr}")
                    break
    for failure in failures:
        print(failure)
    print(f"Docs={len(DOCS)} complete Go packages={len(packages)} failures={len(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
