#!/bin/sh
# Regenerate docs/ from current source using the JDK javadoc tool directly
# (no Maven required).
set -e

# Locate a javadoc binary: prefer one on PATH, else fall back to the
# in-repo JDK downloaded by run_tests.sh.
if command -v javadoc >/dev/null 2>&1; then
    JAVADOC=javadoc
else
    JAVADOC=$(find target/jdk -type f -name javadoc -path '*/bin/*' 2>/dev/null | head -n 1)
    if [ -z "$JAVADOC" ]; then
        echo "error: no javadoc binary found on PATH or under target/jdk." >&2
        echo "Run ./run_tests.sh first to download the JDK, then retry." >&2
        exit 1
    fi
fi

# Move the current docs out of the way so generation always starts from a
# fresh directory; only remove the old copy once generation succeeds.
if [ -d docs ]; then
    mv docs docs_old
fi

"$JAVADOC" \
    -d docs \
    -sourcepath src/main/java \
    -subpackages io.github.elimelt.pmqueue \
    -quiet \
    -notimestamp

rm -rf docs_old
