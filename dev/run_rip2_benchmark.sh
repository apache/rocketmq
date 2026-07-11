#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: dev/run_rip2_benchmark.sh <label> <jmh-include-regex> [output-directory]

Runs exactly one RIP-2 JMH method and captures JSON, JFR, GC, process-time,
stdout, build log, environment, classpath, command, and SHA-256 evidence. The
default output directory is target/rip2-benchmark-results/<label> and must be
empty.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage >&2
  exit 2
fi

LABEL="$1"
BENCHMARK_INCLUDE="$2"
if [[ ! "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "label must contain only letters, digits, dot, underscore, or hyphen" >&2
  exit 2
fi
if [[ ! -f pom.xml || ! -d proxy ]]; then
  echo "run this script from the RocketMQ repository root" >&2
  exit 2
fi

SOURCE_PATHS=(pom.xml dev/run_rip2_benchmark.sh proxy/src/main proxy/src/test)
SOURCE_STATUS="$(git status --short --untracked-files=all -- "${SOURCE_PATHS[@]}")"
if [[ -n "$SOURCE_STATUS" ]]; then
  echo "benchmark source paths are dirty; commit or clean them before recording evidence:" >&2
  printf '%s\n' "$SOURCE_STATUS" >&2
  exit 2
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="$(command -v java)"
fi
command -v mvn >/dev/null
if [[ ! -x "$JAVA" ]]; then
  echo "java is not executable: $JAVA" >&2
  exit 2
fi

# Clean the measured module without deleting previously captured evidence under
# the reactor root's target directory. The following -am test-compile rebuilds
# the complete dependency path required by the proxy benchmark.
mvn -pl proxy clean -DskipTests -DskipITs

OUTPUT_DIR="${3:-target/rip2-benchmark-results/$LABEL}"
if [[ -d "$OUTPUT_DIR" ]] && [[ -n "$(find "$OUTPUT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $OUTPUT_DIR" >&2
  exit 2
fi
mkdir -p "$OUTPUT_DIR"
OUTPUT_ABS="$(cd "$OUTPUT_DIR" && pwd)"

CLASSPATH_FILE="$OUTPUT_ABS/classpath.txt"
JMH_JSON="$OUTPUT_ABS/jmh.json"
JMH_STDOUT="$OUTPUT_ABS/jmh.log"
JFR_FILE="$OUTPUT_ABS/recording.jfr"
GC_LOG="$OUTPUT_ABS/gc.log"
TIME_LOG="$OUTPUT_ABS/time.txt"
COMMAND_FILE="$OUTPUT_ABS/command.txt"
MANIFEST_FILE="$OUTPUT_ABS/SHA256SUMS"
BUILD_LOG="$OUTPUT_ABS/build.log"
ENVIRONMENT_FILE="$OUTPUT_ABS/environment.txt"
SOURCE_FILES_FILE="$OUTPUT_ABS/source-files.txt"
RUNNER_FILE="$OUTPUT_ABS/runner.sh"

mvn -pl proxy -am -DskipTests test-compile -DskipITs | tee "$BUILD_LOG"
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile="$CLASSPATH_FILE" | tee -a "$BUILD_LOG"

CLASSPATH="proxy/target/test-classes:proxy/target/classes:$(cat "$CLASSPATH_FILE")"
MATCH_COUNT="$($JAVA -cp "$CLASSPATH" org.openjdk.jmh.Main -l "$BENCHMARK_INCLUDE" \
  | grep -c '^org\.apache\.rocketmq\.' || true)"
if [[ "$MATCH_COUNT" -ne 1 ]]; then
  echo "JMH include must match exactly one benchmark, matched $MATCH_COUNT" >&2
  exit 2
fi

git ls-files -s -- "${SOURCE_PATHS[@]}" > "$SOURCE_FILES_FILE"
cp dev/run_rip2_benchmark.sh "$RUNNER_FILE"

HEAP_SIZE="${RIP2_HEAP_SIZE:-4g}"
CLIENT_COUNT="${RIP2_CLIENT_COUNT:-1000000}"
GROUP_COUNT="${RIP2_GROUP_COUNT:-1000}"
TOPIC_COUNT="${RIP2_TOPIC_COUNT:-10000}"
PROXY_COUNT="${RIP2_PROXY_COUNT:-100}"
WARMUP_ITERATIONS="${RIP2_WARMUP_ITERATIONS:-1}"
MEASUREMENT_ITERATIONS="${RIP2_MEASUREMENT_ITERATIONS:-3}"
WARMUP_TIME="${RIP2_WARMUP_TIME:-2s}"
MEASUREMENT_TIME="${RIP2_MEASUREMENT_TIME:-3s}"
THREADS="${RIP2_THREADS:-4}"

{
  printf 'recorded_at_utc=' && date -u '+%Y-%m-%dT%H:%M:%SZ'
  printf 'git_head=' && git rev-parse HEAD
  printf 'git_tree=' && git rev-parse HEAD^{tree}
  printf '%s\n' 'git_status_begin'
  git status --short --untracked-files=all
  printf '%s\n' 'git_status_end'
  uname -a
  "$JAVA" -version 2>&1
  mvn -version
  printf 'client_count=%s\n' "$CLIENT_COUNT"
  printf 'group_count=%s\n' "$GROUP_COUNT"
  printf 'topic_count=%s\n' "$TOPIC_COUNT"
  printf 'proxy_count=%s\n' "$PROXY_COUNT"
  printf 'heap_size=%s\n' "$HEAP_SIZE"
  printf 'threads=%s\n' "$THREADS"
  printf 'warmup_iterations=%s\n' "$WARMUP_ITERATIONS"
  printf 'measurement_iterations=%s\n' "$MEASUREMENT_ITERATIONS"
  printf 'warmup_time=%s\n' "$WARMUP_TIME"
  printf 'measurement_time=%s\n' "$MEASUREMENT_TIME"
} > "$ENVIRONMENT_FILE"

JVM_ARGS="-Xms$HEAP_SIZE -Xmx$HEAP_SIZE -XX:+UseG1GC"
JVM_ARGS+=" -XX:StartFlightRecording=filename=$JFR_FILE,settings=profile,dumponexit=true"
JVM_ARGS+=" -Xlog:gc*:file=$GC_LOG:time,uptime,level,tags"

printf '%q ' "$JAVA" -cp "$CLASSPATH" org.openjdk.jmh.Main "$BENCHMARK_INCLUDE" \
  -p "clientCount=$CLIENT_COUNT" -p "groupCount=$GROUP_COUNT" \
  -p "topicCount=$TOPIC_COUNT" -p "proxyCount=$PROXY_COUNT" \
  -wi "$WARMUP_ITERATIONS" -i "$MEASUREMENT_ITERATIONS" \
  -w "$WARMUP_TIME" -r "$MEASUREMENT_TIME" -f 1 -t "$THREADS" \
  -jvmArgsAppend "$JVM_ARGS" -prof gc -rf json -rff "$JMH_JSON" \
  > "$COMMAND_FILE"
printf '\n' >> "$COMMAND_FILE"

TIME_ARGS=(-v)
if [[ "$(uname -s)" == "Darwin" ]]; then
  TIME_ARGS=(-l)
fi
/usr/bin/time "${TIME_ARGS[@]}" -o "$TIME_LOG" \
  "$JAVA" -cp "$CLASSPATH" org.openjdk.jmh.Main "$BENCHMARK_INCLUDE" \
  -p "clientCount=$CLIENT_COUNT" -p "groupCount=$GROUP_COUNT" \
  -p "topicCount=$TOPIC_COUNT" -p "proxyCount=$PROXY_COUNT" \
  -wi "$WARMUP_ITERATIONS" -i "$MEASUREMENT_ITERATIONS" \
  -w "$WARMUP_TIME" -r "$MEASUREMENT_TIME" -f 1 -t "$THREADS" \
  -jvmArgsAppend "$JVM_ARGS" -prof gc -rf json -rff "$JMH_JSON" \
  > "$JMH_STDOUT" 2>&1

ARTIFACTS=(build.log classpath.txt command.txt environment.txt gc.log jmh.json jmh.log recording.jfr runner.sh source-files.txt time.txt)
for artifact in "${ARTIFACTS[@]}"; do
  test -s "$OUTPUT_ABS/$artifact"
done
if command -v sha256sum >/dev/null; then
  (cd "$OUTPUT_ABS" && sha256sum "${ARTIFACTS[@]}" > SHA256SUMS)
else
  (cd "$OUTPUT_ABS" && shasum -a 256 "${ARTIFACTS[@]}" > SHA256SUMS)
fi
test -s "$MANIFEST_FILE"

echo "RIP-2 benchmark evidence written to $OUTPUT_DIR"
