#!/usr/bin/env bash

set -o errtrace -o nounset -o pipefail -o errexit

# Goto directory of this script
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "Executing self-check"
# Don't fail here, failing later at the end when all shell scripts are checked anyway.
shellcheck ./ci.sh && echo "Self-check succeeded!" || echo "Self-check failed!"

echo "Executing unit tests"
./sbt test

echo "Executing integration tests"
./sbt it:test

echo "Executing shellcheck against all included shell scripts"
find . -name "*.sh" -print0 | xargs -n 1 -0 shellcheck