#!/usr/bin/env bash
set -euo pipefail

if ! command -v java >/dev/null 2>&1; then
  echo "Java is not available. Rebuild the Codespace so the devcontainer Java feature can install Java 17." >&2
  return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
export PATH="$JAVA_HOME/bin:$PATH"

echo "Java environment configured for Codespaces:"
which java
java -version
echo "$JAVA_HOME"
