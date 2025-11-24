#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$(readlink -f "$SCRIPT_DIR/../../..")"
TOOLS_RUN_ALLOC="$REPO_ROOT/tools/run_allocated.sh"

if [ ! -x "$TOOLS_RUN_ALLOC" ]; then
    echo "[run_allocated wrapper] Unable to find $TOOLS_RUN_ALLOC" >&2
    exit 1
fi

echo "[run_allocated wrapper] Delegating to tools/run_allocated.sh"
exec "$TOOLS_RUN_ALLOC" "$@"
