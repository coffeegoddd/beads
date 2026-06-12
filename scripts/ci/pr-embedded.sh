#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$REPO_ROOT/.buildflags"
source "$REPO_ROOT/scripts/ci/lib/timing.sh"
source "$REPO_ROOT/scripts/ci/lib/test-env.sh"

cd "$REPO_ROOT"

beads_test_env_enter

if [[ -z "${BEADS_TEST_BD_BINARY:-}" ]]; then
    echo "ERROR: BEADS_TEST_BD_BINARY must point at a prebuilt bd binary" >&2
    exit 2
fi

export BEADS_TEST_EMBEDDED_DOLT=1

GO_TEST_PKG_PARALLEL="${GO_TEST_PKG_PARALLEL:-2}"
GO_TEST_PARALLEL="${GO_TEST_PARALLEL:-2}"

ci_time "pr-embedded go test" -- \
    go test -p "$GO_TEST_PKG_PARALLEL" -parallel "$GO_TEST_PARALLEL" -race -timeout 30m \
        -run '^TestEmbedded' ./cmd/bd
