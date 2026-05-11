#!/usr/bin/env bash
set -euo pipefail

FORBIDDEN=(tokio hyper reqwest tower axum async-std smol)

tree_output=$(cargo tree -p reckon-cli --prefix none --no-dedupe 2>&1)

found=0
for crate in "${FORBIDDEN[@]}"; do
    if echo "$tree_output" | grep -qE "^${crate} v"; then
        echo "FORBIDDEN: ${crate} found in reckon-cli dependency tree" >&2
        found=1
    fi
done

if [ "$found" -eq 1 ]; then
    exit 1
fi

echo "OK: no forbidden async runtimes in reckon-cli"
