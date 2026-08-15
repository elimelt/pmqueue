#!/usr/bin/env bash
# Compile and run the full test suite without Maven.
# Downloads (once, into gitignored target/):
#   - a Temurin JDK (target/jdk/)
#   - JUnit console launcher + Mockito and friends (target/deps/)
# Usage: ./run_tests.sh
set -euo pipefail

cd "$(dirname "$0")"

JAVA_RELEASE=21
DEPS_DIR=target/deps
JDK_DIR=target/jdk
CLASSES=target/classes
TEST_CLASSES=target/test-classes

# --- JDK ---------------------------------------------------------------
find_javac() {
  find "$JDK_DIR" -type f -name javac -path '*/bin/*' 2>/dev/null | head -1
}

JAVAC="$(find_javac || true)"
if [ -z "${JAVAC:-}" ]; then
  case "$(uname -s)" in
    Darwin) os=mac ;;
    Linux) os=linux ;;
    *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch=aarch64 ;;
    x86_64) arch=x64 ;;
    *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
  esac
  echo "Downloading Temurin $JAVA_RELEASE JDK ($os/$arch) into $JDK_DIR ..."
  mkdir -p "$JDK_DIR"
  curl -fsSL -o "$JDK_DIR/jdk.tar.gz" \
    "https://api.adoptium.net/v3/binary/latest/$JAVA_RELEASE/ga/$os/$arch/jdk/hotspot/normal/eclipse"
  tar -xzf "$JDK_DIR/jdk.tar.gz" -C "$JDK_DIR"
  rm "$JDK_DIR/jdk.tar.gz"
  JAVAC="$(find_javac)"
fi
[ -n "$JAVAC" ] || { echo "JDK setup failed" >&2; exit 1; }
JAVA="$(dirname "$JAVAC")/java"
"$JAVA" -version

# --- Test dependencies --------------------------------------------------
MAVEN_CENTRAL=https://repo1.maven.org/maven2
deps=(
  org/junit/platform/junit-platform-console-standalone/1.10.2/junit-platform-console-standalone-1.10.2.jar
  org/mockito/mockito-core/5.11.0/mockito-core-5.11.0.jar
  net/bytebuddy/byte-buddy/1.14.12/byte-buddy-1.14.12.jar
  net/bytebuddy/byte-buddy-agent/1.14.12/byte-buddy-agent-1.14.12.jar
  org/objenesis/objenesis/3.3/objenesis-3.3.jar
)
mkdir -p "$DEPS_DIR"
for dep in "${deps[@]}"; do
  jar="$DEPS_DIR/$(basename "$dep")"
  if [ ! -f "$jar" ]; then
    echo "Downloading $(basename "$dep") ..."
    curl -fsSL -o "$jar" "$MAVEN_CENTRAL/$dep"
  fi
done
JUNIT_CONSOLE="$DEPS_DIR/junit-platform-console-standalone-1.10.2.jar"
DEP_CP="$(printf '%s:' "$DEPS_DIR"/*.jar)"
DEP_CP="${DEP_CP%:}"

# --- Compile ------------------------------------------------------------
rm -rf "$CLASSES" "$TEST_CLASSES"
mkdir -p "$CLASSES" "$TEST_CLASSES"
echo "Compiling main sources ..."
find src/main -name '*.java' -print0 | xargs -0 "$JAVAC" --release "$JAVA_RELEASE" \
  -d "$CLASSES"
echo "Compiling test sources ..."
find src/test -name '*.java' -print0 | xargs -0 "$JAVAC" --release "$JAVA_RELEASE" \
  -nowarn -cp "$CLASSES:$DEP_CP" -d "$TEST_CLASSES"

# --- Run ----------------------------------------------------------------
echo "Running tests ..."
"$JAVA" -jar "$JUNIT_CONSOLE" execute \
  -cp "$CLASSES:$TEST_CLASSES:$DEP_CP" \
  --scan-classpath \
  --fail-if-no-tests \
  --disable-ansi-colors
