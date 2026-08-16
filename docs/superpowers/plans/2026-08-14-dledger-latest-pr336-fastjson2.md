# Latest DLedger PR #336 RocketMQ Fastjson2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt current RocketMQ `develop` to current DLedger plus PR #336, remove fastjson1 from both Maven and Bazel resolution, and prove storage, controller, broker restart, failover, and JSON compatibility on JDK 8.

**Architecture:** Build two isolated DLedger artifacts from the same latest API line—one without #336 for RED evidence and one with #336 for GREEN—then port RocketMQ by intent from #10650. RocketMQ derives its committed physical boundary from DLedger index metadata, enables DLedger's safe fast-advance mechanism after restart, treats an internal DLedger NOOP as physical framing that advances recovery/reput without dispatching a RocketMQ message, keeps the reactor's own remoting implementation, and validates old fastjson1 wire data with immutable golden fixtures rather than loading fastjson1.

**Tech Stack:** Java 8 (Amazon Corretto 8.502), Maven 3.9.x, Bazel 6.5.0, JUnit 4/5, Awaitility, RocketMQ `develop`, DLedger `master` plus PR #336, fastjson2 2.0.64.

---

## Fixed inputs and safety rules

- RocketMQ repository: `/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源`
- RocketMQ base: `293f5885719fc4aa3619446a1900f58ccfcfdd29`
- RocketMQ branch: `codex/dledger-latest-pr336-adapter`
- DLedger repository: `/private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger`
- DLedger master: `2834424cec15670c1b1108378b0b0a3a5fd57791`
- DLedger PR #336: `a2555fa78fcda379279a815357717283d0f6c56b`
- Corretto 8: `/private/tmp/rocketmq-dledger-fastjson2/tools/amazon-corretto-8.jdk/Contents/Home`
- Bazel: `/private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0`
- Validation root: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation`
- Isolated Maven repository: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2`
- RocketMQ 0.3.2 fixture worktree: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer`
- RocketMQ latest reader worktree: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader`
- DLedger RED coordinate: `io.openmessaging.storage:dledger:0.3.3-master-f2-64-SNAPSHOT`
- DLedger GREEN/final coordinate: `io.openmessaging.storage:dledger:0.3.3-pr336-f2-64-SNAPSHOT`

Never read, modify, delete, stage, or commit the pre-existing untracked `broker/null/` directory. Do not change the existing maintenance-line branches, PR #337, or PR #10928. Do not push this RocketMQ branch while its POM and WORKSPACE refer to the local-only DLedger validation coordinate.

Mixed DLedger 0.3.2/current-master rolling upgrade and direct downgrade are explicitly out of scope. The final report must describe this as a coordinated full-stop validation only.

## Task 1: Freeze baselines and create isolated DLedger RED/GREEN worktrees

**Files:**

- Inspect: `/private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger/.git`
- Create worktree: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-master-red`
- Create worktree: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green`
- Create worktree: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer`
- Test-only edit in both worktrees: `dledger/src/test/java/io/openmessaging/storage/dledger/util/FileTestUtil.java`
- Test-only edit in both worktrees: `proxy/src/test/java/io/openmessaging/storage/dledger/proxy/util/FileTestUtil.java`

- [ ] **Step 1: Verify every immutable input before creating anything**

Run:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
test "$(git rev-parse HEAD^)" = 85872858ec38a3a1f99a7e2d61f7f39406136818 || exit 1
test "$(git rev-parse HEAD^^)" = 293f5885719fc4aa3619446a1900f58ccfcfdd29 || exit 1
git diff --quiet 85872858ec38a3a1f99a7e2d61f7f39406136818...HEAD \
  -- . ':!docs/superpowers/plans/2026-08-14-dledger-latest-pr336-fastjson2.md' ':!broker/null'
git status --short --branch

git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger fetch origin \
  master refs/pull/336/head:refs/remotes/origin/pr-336
test "$(git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger rev-parse origin/master)" \
  = 2834424cec15670c1b1108378b0b0a3a5fd57791 || exit 1
test "$(git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger rev-parse origin/pr-336)" \
  = a2555fa78fcda379279a815357717283d0f6c56b || exit 1
test "$(git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger rev-parse origin/pr-336^)" \
  = 2834424cec15670c1b1108378b0b0a3a5fd57791 || exit 1
```

Expected RocketMQ status is exactly the branch header plus `?? broker/null/`. Stop if any tracked file is modified or either remote SHA differs.

- [ ] **Step 2: Create the low-usage validation directories and detached worktrees**

Run read-only occupancy checks first. If either exact target exists, stop rather than deleting it.

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-master-red
test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green
test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer
test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader
mkdir -p /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/{m2,tmp,dledger-test-data,logs}
df -h /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation

git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger worktree add --detach \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-master-red \
  2834424cec15670c1b1108378b0b0a3a5fd57791
git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger worktree add --detach \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green \
  a2555fa78fcda379279a815357717283d0f6c56b
git worktree add --detach \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer \
  293f5885719fc4aa3619446a1900f58ccfcfdd29
test -z "$(git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer status --short)"
rg -F '<dleger.version>0.3.2</dleger.version>' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer/pom.xml
rg -F '<artifactId>fastjson</artifactId>' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer/common/pom.xml
```

The backing volume must be below DLedger's 95% disk-full threshold.

Using `apply_patch`, create `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8` with:

```sh
#!/bin/sh
export JAVA_HOME=/private/tmp/rocketmq-dledger-fastjson2/tools/amazon-corretto-8.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
exec "$@"
```

Using `apply_patch`, create `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged` with:

```bash
#!/bin/bash
set -o pipefail
log_file=$1
shift
"$@" 2>&1 | tee "$log_file"
```

Using `apply_patch`, create `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report` with:

```bash
#!/bin/bash
set -euo pipefail
report=$1
expected_tests=$2
start_marker=$3
test -s "$report"
test "$report" -nt "$start_marker"
tests=$(/usr/bin/xmllint --xpath 'string(/testsuite/@tests)' "$report")
failures=$(/usr/bin/xmllint --xpath 'string(/testsuite/@failures)' "$report")
errors=$(/usr/bin/xmllint --xpath 'string(/testsuite/@errors)' "$report")
skipped=$(/usr/bin/xmllint --xpath 'string(/testsuite/@skipped)' "$report")
test "$tests" -eq "$expected_tests"
test "$failures" -eq 0
test "$errors" -eq 0
test "$skipped" -eq 0
```

The marker timestamp prevents a stale XML report from satisfying a test-selection gate. Every focused GREEN or integration command below creates its own marker immediately before Maven and calls this helper afterward.

Then run:

```bash
set -euo pipefail
chmod 755 \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 java -version
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 mvn -version
```

Both commands must report Corretto 8.502. Every later Maven invocation goes through `run-jdk8`; every logged command goes through `run-logged`, whose `pipefail` preserves the wrapped command's exit status instead of returning `tee`'s status.

- [ ] **Step 3: Make DLedger's hard-coded test root configurable in the disposable worktrees only**

In both disposable worktrees, update both the `dledger` and `proxy` copies of `FileTestUtil.java`. Replace each hard-coded constant with:

```java
public static final String TEST_BASE = System.getProperty(
    "dledger.test.base", File.separator + "tmp" + File.separator + "dledgerteststore");
```

This is a local test harness change. It must never be committed, included in the installed artifact's production bytecode, or applied to the original DLedger worktree.

- [ ] **Step 4: Add only PR #336's assertions to the master RED worktree**

In `dledger/src/test/java/io/openmessaging/storage/dledger/AppendAndPushTest.java`, add these assertions without adding the production `future.setPos` line:

```java
Assertions.assertEquals((DLedgerEntry.BODY_OFFSET + 256L) * i,
    ((AppendFuture<AppendEntryResponse>) future).getPos());
```

and:

```java
Assertions.assertEquals(positions[count - 1],
    ((BatchAppendFuture<AppendEntryResponse>) future).getPos());
```

- [ ] **Step 5: Capture the DLedger RED**

Run:

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-master-red
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-master-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu -pl dledger \
  -Djacoco.skip=true \
  -Ddledger.test.base=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-test-data/master-red \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Dtest=AppendAndPushTest clean test
```

Expected: non-zero exit and both new assertions report actual `-1`. If the failure is `DISK_FULL`, a port collision, dependency resolution, or any assertion other than the future position, fix the environment and rerun before proceeding.

After the expected non-zero command, prove the log contains both position failures and no environment failure:

```bash
set -euo pipefail
LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-master-red.log
test "$(rg -c 'expected: <[0-9]+> but was: <-1>' "$LOG")" -ge 2
if rg 'DISK_FULL|BindException|Address already in use' "$LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

- [ ] **Step 6: Run the exact GREEN test on PR #336**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-green.marker
: > "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu -pl dledger \
  -Djacoco.skip=true \
  -Ddledger.test.base=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-test-data/pr336-green \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Dtest=AppendAndPushTest clean test
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report \
  dledger/target/surefire-reports/TEST-io.openmessaging.storage.dledger.AppendAndPushTest.xml \
  6 "$MARKER"
```

Expected: `AppendAndPushTest` passes, including the single immediate position and batch last-position assertions.

## Task 2: Build exact DLedger master and PR #336 consumer artifacts with fastjson2 2.0.64

**Files:**

- Modify only in each disposable worktree: `pom.xml`
- Modify only in each disposable worktree: `dledger/pom.xml`
- Modify only in each disposable worktree: `proxy/pom.xml`
- Modify only in each disposable worktree: `example/pom.xml`
- Output: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2/io/openmessaging/storage/dledger/**`

- [ ] **Step 1: Apply the build-only master overlay**

In `dledger-master-red`:

- Change root `dledger-all` version from `0.3.3-SNAPSHOT` to `0.3.3-master-f2-64-SNAPSHOT`.
- Change the parent version in `dledger/pom.xml`, `proxy/pom.xml`, and `example/pom.xml` to `0.3.3-master-f2-64-SNAPSHOT`.
- Change the direct `com.alibaba.fastjson2:fastjson2` version in `dledger/pom.xml` from `2.0.59` to `2.0.64`.
- Leave the `com.alibaba:fastjson` exclusion in place.

Do not add PR #336's production line to this worktree.

- [ ] **Step 2: Apply the build-only PR #336 overlay**

In `dledger-pr336-green`, make these four explicit build-only edits:

- Change the root `dledger-all` version from `0.3.3-SNAPSHOT` to `0.3.3-pr336-f2-64-SNAPSHOT`.
- Change the parent version in `dledger/pom.xml`, `proxy/pom.xml`, and `example/pom.xml` to `0.3.3-pr336-f2-64-SNAPSHOT`.
- Change the direct `com.alibaba.fastjson2:fastjson2` version in `dledger/pom.xml` from `2.0.59` to `2.0.64`.
- Leave the `com.alibaba:fastjson` exclusion in place and make no production-source changes beyond PR #336's existing commit.

- [ ] **Step 3: Add the existing isolated RPC regression harness to the disposable PR worktree**

The exact three-test JUnit 5 harness already exists in local commit `be90b95caf5d659a69f2ddce751d61f49021058c`. Inspect it with:

```bash
set -euo pipefail
git -C /private/tmp/rocketmq-pr10650-dledger.MbRO1B/dledger show \
  be90b95caf5d659a69f2ddce751d61f49021058c:dledger/src/test/java/io/openmessaging/storage/dledger/DLedgerRpcNettyServiceTest.java
```

Using `apply_patch`, add that exact file to `dledger-pr336-green`. It covers:

- fresh-JVM cold start of server and client RPC classes;
- a binary body `{0, 1, 0xFF, 65}` through real append/get networking;
- an append future completing with `NETWORK_ERROR` when connection establishment fails.

This is a disposable validation test only; do not commit it to DLedger PR #336.

- [ ] **Step 4: Run the RPC harness with JDK 8 and fastjson2 2.0.64**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-rpc.marker
: > "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-rpc.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu -pl dledger \
  -Djacoco.skip=true \
  -Ddledger.test.base=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-test-data/pr336-rpc \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Dtest=DLedgerRpcNettyServiceTest test
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report \
  dledger/target/surefire-reports/TEST-io.openmessaging.storage.dledger.DLedgerRpcNettyServiceTest.xml \
  3 "$MARKER"
```

Expected: all three tests pass; the connection-failure future completes within five seconds rather than hanging.

- [ ] **Step 5: Run the complete PR #336 reactor with the fastjson2 2.0.64 overlay**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-full-test.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Djacoco.skip=true \
  -Ddledger.test.base=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-test-data/pr336-full \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  clean test
```

Expected: the `dledger`, `dledger-proxy`, and example reactor tests all pass with effective fastjson2 2.0.64. Retain the complete log; do not accept a tail-only summary.

- [ ] **Step 6: Install both artifacts using JDK 8**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-master-red
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-master-install.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu -DskipTests -Djacoco.skip=true -Dgpg.skip=true \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  clean install

cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-install.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu -DskipTests -Djacoco.skip=true -Dgpg.skip=true \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  clean install
```

Expected: both four-module reactors report `BUILD SUCCESS`.

- [ ] **Step 7: Audit the PR #336 artifact before RocketMQ consumes it**

```bash
set -euo pipefail
DL_JAR=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2/io/openmessaging/storage/dledger/0.3.3-pr336-f2-64-SNAPSHOT/dledger-0.3.3-pr336-f2-64-SNAPSHOT.jar
DL_ENTRIES=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-jar-entries.log
test -s "$DL_JAR"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-jar.sha256 \
  shasum -a 256 "$DL_JAR"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  jar tf "$DL_JAR" > "$DL_ENTRIES"
test -s "$DL_ENTRIES"
rg -F 'io/openmessaging/storage/dledger/common/AppendFuture.class' "$DL_ENTRIES"
rg -F 'io/openmessaging/storage/dledger/common/BatchAppendFuture.class' "$DL_ENTRIES"
rg -F 'io/openmessaging/storage/dledger/statemachine/ApplyEntry.class' "$DL_ENTRIES"
rg -F 'io/openmessaging/storage/dledger/statemachine/ApplyEntryIterator.class' "$DL_ENTRIES"
if rg '^com/alibaba/fastjson/' "$DL_ENTRIES"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi

/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-dependencies.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -f /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/dledger-pr336-green/dledger/pom.xml \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  dependency:tree \
  -Dverbose \
  -Dincludes=com.alibaba:fastjson,com.alibaba.fastjson2:fastjson2,org.apache.rocketmq:rocketmq-remoting
```

Expected dependency tree: one `rocketmq-remoting:5.5.0`, one fastjson2 `2.0.64`, and zero resolved `com.alibaba:fastjson` artifacts. The only permitted fastjson1 text is the POM exclusion.

## Task 3: Add the fastjson compatibility RED before changing RocketMQ dependencies

**Files:**

- Modify: `remoting/src/test/java/org/apache/rocketmq/remoting/protocol/RemotingSerializableCompatTest.java`

- [ ] **Step 1: Record the current fastjson1 footprint**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-fastjson1-red-scan.log \
  rg -n --pcre2 \
  'com\.alibaba:fastjson(?!2)|com\.alibaba\.fastjson(?!2)|com_alibaba_fastjson(?!2)|<fastjson\.version>|<artifactId>fastjson</artifactId>' \
  pom.xml common/pom.xml WORKSPACE remoting \
  --glob '!**/target/**'
```

Expected: the root property/dependency management, `common/pom.xml`, WORKSPACE artifact, Bazel test dependency, annotation import, and live fastjson1 serializer are all present.

- [ ] **Step 2: Add the fresh-JVM cold-start test only**

Add the imports:

```java
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
```

Add this test and probe to `RemotingSerializableCompatTest`:

```java
@Test
public void testRemotingCodecColdStart() throws Exception {
    String javaExecutable = System.getProperty("java.home")
        + File.separator + "bin" + File.separator + "java";
    String classPath = System.getProperty(
        "surefire.test.class.path", System.getProperty("java.class.path"));

    ProcessBuilder processBuilder = new ProcessBuilder(
        javaExecutable, "-cp", classPath, ColdStartProbe.class.getName());
    processBuilder.environment().remove("JAVA_TOOL_OPTIONS");
    processBuilder.environment().remove("_JAVA_OPTIONS");
    processBuilder.environment().remove("JDK_JAVA_OPTIONS");

    Process process = processBuilder.redirectErrorStream(true).start();
    boolean finished = process.waitFor(10, TimeUnit.SECONDS);
    if (!finished) {
        process.destroyForcibly();
        fail("Cold-start probe did not finish");
    }

    StringBuilder output = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line).append(System.lineSeparator());
        }
    }
    assertEquals(output.toString(), 0, process.exitValue());
}

public static final class ColdStartProbe {
    private ColdStartProbe() {
    }

    public static void main(String[] args) {
        try {
            ConsumerConnection connection = new ConsumerConnection();
            String json = RemotingSerializable.toJson(connection, false);
            ConsumerConnection decoded =
                RemotingSerializable.fromJson(json, ConsumerConnection.class);
            if (decoded == null || decoded.getConnectionSet() == null) {
                throw new AssertionError(
                    "Remoting codec returned an incomplete ConsumerConnection: " + json);
            }
            Runtime.getRuntime().halt(0);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.err.flush();
            Runtime.getRuntime().halt(1);
        }
    }
}
```

Add the `ConsumerConnection` import now, but leave RocketMQ's effective fastjson2 version at 2.0.63 for this RED.

- [ ] **Step 3: Capture the JDK 8 fastjson2 2.0.63 RED**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-fastjson2-2.0.63-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Djacoco.skip=true \
  -pl remoting -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=RemotingSerializableCompatTest#testRemotingCodecColdStart' \
  clean test
```

Expected: non-zero exit; the child JVM reports `LambdaConversionException: Invalid caller`. A compile failure or unrelated test failure is not the required RED.

```bash
set -euo pipefail
LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-fastjson2-2.0.63-red.log
rg -F 'java.lang.invoke.LambdaConversionException: Invalid caller' "$LOG"
if rg 'COMPILATION ERROR|DISK_FULL|BindException|Address already in use' "$LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

## Task 4: Remove fastjson1, align fastjson2 2.0.64, and capture the latest-DLedger compile RED

**Files:**

- Modify: `pom.xml`
- Modify: `common/pom.xml`
- Modify: `WORKSPACE`
- Modify: `remoting/BUILD.bazel`
- Modify: `remoting/src/test/java/org/apache/rocketmq/remoting/protocol/RemotingSerializableCompatTest.java`

- [ ] **Step 1: Update Maven and Bazel dependency metadata together**

Apply these exact changes:

```xml
<fastjson2.version>2.0.64</fastjson2.version>
<dleger.version>0.3.3-pr336-f2-64-SNAPSHOT</dleger.version>
```

- Delete `<fastjson.version>1.2.83</fastjson.version>`.
- Delete the `com.alibaba:fastjson` dependency from root dependency management.
- Delete the `com.alibaba:fastjson` dependency from `common/pom.xml`.
- In WORKSPACE delete `com.alibaba:fastjson:1.2.83`, change fastjson2 to `2.0.64`, and change DLedger to `0.3.3-pr336-f2-64-SNAPSHOT`.
- Add this to `maven_install`:

```starlark
excluded_artifacts = [
    "org.apache.rocketmq:rocketmq-remoting",
]
```

- Delete `@maven//:com_alibaba_fastjson` from the remoting test target.

The remoting exclusion is mandatory: RocketMQ modules must use the reactor's `//remoting`, not DLedger's transitive external `rocketmq-remoting:5.5.0` JAR.

- [ ] **Step 2: Replace the live fastjson1 oracle with immutable 1.2.83 fixtures**

These literals were generated with `com.alibaba:fastjson:1.2.83` at RocketMQ commit `47c6e89589183bb0cc95463b94495aaac5cee9f8`, the direct parent of the fastjson2 migration commit `d7e27d6d6988a0cd98100097c9207ec0f5a9f83c`. They are checked-in protocol fixtures, not output regenerated by the new serializer.

Change the annotation import to `com.alibaba.fastjson2.annotation.JSONField`, remove direct `com.alibaba.fastjson.JSON` and `com.alibaba.fastjson2.JSON` calls, and add the following constants:

```java
private static final String FASTJSON1_BATCH_ACK =
    "{\"b\":\"Kg==\",\"c\":\"fixture-consumer\",\"it\":60000,"
        + "\"pt\":1700000000123,\"q\":7,\"r\":\"0\",\"rq\":3,"
        + "\"so\":1234567890123,\"t\":\"FixtureTopic\"}";

private static final String FASTJSON1_SUBSCRIPTION_DATA =
    "{\"classFilterMode\":true,\"codeSet\":[101,202],\"expressionType\":\"SQL92\","
        + "\"subString\":\"TagA || TagB\",\"subVersion\":1700000000456,"
        + "\"tagsSet\":[\"TagA\",\"TagB\"],\"topic\":\"FixtureTopic\"}";

private static final String FASTJSON1_CONSUMER_CONNECTION =
    "{\"connectionSet\":[{\"clientAddr\":\"127.0.0.1:10911\","
        + "\"clientId\":\"fixture-client@instance-1\",\"language\":\"GO\",\"version\":433}],"
        + "\"consumeFromWhere\":\"CONSUME_FROM_TIMESTAMP\","
        + "\"consumeType\":\"CONSUME_PASSIVELY\",\"messageModel\":\"CLUSTERING\","
        + "\"subscriptionTable\":{\"FixtureTopic\":"
        + FASTJSON1_SUBSCRIPTION_DATA + "}}";
```

Replace the existing `testCompatibilityCheckWithBitSet` method with the BatchAck fixture test below, then add the SubscriptionData and ConsumerConnection fixture tests. All three parse only through `RemotingSerializable.fromJson` and assert every field; do not retain the old BitSet-only method as a sixth duplicate test.

```java
@Test
public void testFastjson1BatchAckFixture() {
    BatchAck batchAck = RemotingSerializable.fromJson(FASTJSON1_BATCH_ACK, BatchAck.class);
    BitSet expected = new BitSet();
    expected.set(1);
    expected.set(3);
    expected.set(5);
    assertEquals(expected, batchAck.getBitSet());
    assertEquals("fixture-consumer", batchAck.getConsumerGroup());
    assertEquals(60000L, batchAck.getInvisibleTime());
    assertEquals(1700000000123L, batchAck.getPopTime());
    assertEquals(7, batchAck.getQueueId());
    assertEquals("0", batchAck.getRetry());
    assertEquals(3, batchAck.getReviveQueueId());
    assertEquals(1234567890123L, batchAck.getStartOffset());
    assertEquals("FixtureTopic", batchAck.getTopic());
}
```

Add the other two fixtures and shared assertion exactly as follows:

```java
@Test
public void testFastjson1SubscriptionDataFixture() {
    SubscriptionData subscriptionData = RemotingSerializable.fromJson(
        FASTJSON1_SUBSCRIPTION_DATA, SubscriptionData.class);
    assertSubscriptionData(subscriptionData);
}

@Test
public void testFastjson1ConsumerConnectionFixture() {
    ConsumerConnection consumerConnection = RemotingSerializable.fromJson(
        FASTJSON1_CONSUMER_CONNECTION, ConsumerConnection.class);
    assertEquals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        consumerConnection.getConsumeFromWhere());
    assertEquals(ConsumeType.CONSUME_PASSIVELY, consumerConnection.getConsumeType());
    assertEquals(MessageModel.CLUSTERING, consumerConnection.getMessageModel());
    assertEquals(1, consumerConnection.getConnectionSet().size());

    Connection connection = consumerConnection.getConnectionSet().iterator().next();
    assertEquals("127.0.0.1:10911", connection.getClientAddr());
    assertEquals("fixture-client@instance-1", connection.getClientId());
    assertEquals(LanguageCode.GO, connection.getLanguage());
    assertEquals(433, connection.getVersion());
    assertEquals(433, consumerConnection.computeMinVersion());
    assertEquals(new HashSet<>(Arrays.asList("FixtureTopic")),
        consumerConnection.getSubscriptionTable().keySet());
    assertSubscriptionData(
        consumerConnection.getSubscriptionTable().get("FixtureTopic"));
}

private void assertSubscriptionData(SubscriptionData subscriptionData) {
    assertTrue(subscriptionData.isClassFilterMode());
    assertEquals("FixtureTopic", subscriptionData.getTopic());
    assertEquals("TagA || TagB", subscriptionData.getSubString());
    assertEquals(new HashSet<>(Arrays.asList("TagA", "TagB")),
        subscriptionData.getTagsSet());
    assertEquals(new HashSet<>(Arrays.asList(101, 202)),
        subscriptionData.getCodeSet());
    assertEquals(1700000000456L, subscriptionData.getSubVersion());
    assertEquals("SQL92", subscriptionData.getExpressionType());
    assertNull(subscriptionData.getFilterClassSource());
}
```

Add imports for `ConsumeFromWhere`, `Connection`, `ConsumerConnection`, `ConsumeType`, `MessageModel`, `SubscriptionData`, `Arrays`, `assertNull`, and `fail`. Set ordering is deliberately excluded from the contract.

In both reflective field loops skip static and transient fields:

```java
if (Modifier.isStatic(field.getModifiers())
    || Modifier.isTransient(field.getModifiers())) {
    continue;
}
```

Change reflective setup failures from print-and-continue to:

```java
throw new AssertionError("Class " + clazz.getName() + " could not be checked", e);
```

Change the round-trip implementation to the production path:

```java
String json = RemotingSerializable.toJson(original, false);
Object deserialized = RemotingSerializable.fromJson(json, clazz);
```

Do not rely on the generic reflective comparator for the golden fixtures; its pre-existing array handling can false-green. The fixture tests' direct assertions are the oracle.

- [ ] **Step 3: Verify the remoting GREEN independently**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
REPORT=remoting/target/surefire-reports/TEST-org.apache.rocketmq.remoting.protocol.RemotingSerializableCompatTest.xml
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-remoting-fastjson2-green.start
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-remoting-fastjson2-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Djacoco.skip=true \
  -pl remoting -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=RemotingSerializableCompatTest \
  clean test
$ASSERT "$REPORT" 5 "$MARKER"
```

Expected: five tests pass, including the fresh-JVM probe and three fastjson1 fixtures.

- [ ] **Step 4: Capture the expected latest-DLedger compile RED before source adaptation**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-latest-dledger-compile-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -pl store,controller,broker,container -am \
  -DskipTests package
```

Expected compile errors must be limited to the changed DLedger API: old Future packages, `CommittedEntryIterator`, `getCommittedPos`, `setEnableBatchPush`, and direct store `getCommittedIndex`. Unexpected errors stop the task.

```bash
set -euo pipefail
LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-latest-dledger-compile-red.log
rg -F 'BUILD FAILURE' "$LOG"
rg -n 'CommittedEntryIterator|io\.openmessaging\.storage\.dledger\.(AppendFuture|BatchAppendFuture)|getCommittedPos|setEnableBatchPush|getCommittedIndex' "$LOG"
if rg 'DISK_FULL|BindException|Address already in use|Could not resolve dependencies' "$LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

## Task 5: Port #10650's API intent and implement a safe committed-position resolver

**Files:**

- Modify: `broker/src/main/java/org/apache/rocketmq/broker/dledger/DLedgerRoleChangeHandler.java`
- Modify: `controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerController.java`
- Modify: `controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerControllerStateMachine.java`
- Modify: `store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java`

- [ ] **Step 1: Port the mechanical latest-API changes**

Make these exact adaptations:

- `AppendFuture` and `BatchAppendFuture` import from `io.openmessaging.storage.dledger.common`.
- In the role handler compare `getDLedgerStore().getLedgerEndIndex()` with `getMemberState().getCommittedIndex()`.
- Replace `CommittedEntryIterator` with `ApplyEntryIterator`.
- Unwrap each state-machine item with `iterator.next().getEntry()`.
- Replace the `setEnableBatchPush` configuration call with `setEnableBatchAppend` while preserving the `MessageStoreConfig#isEnableBatchPush` source value.

The state-machine loop must remain one event per DLedger entry:

```java
@Override
public void onApply(ApplyEntryIterator iterator) {
    int applyingSize = 0;
    long firstApplyIndex = -1;
    long lastApplyIndex = -1;
    while (iterator.hasNext()) {
        final ApplyEntry applyEntry = iterator.next();
        final DLedgerEntry entry = applyEntry.getEntry();
        final byte[] body = entry.getBody();
        if (body != null && body.length > 0) {
            final EventMessage event = this.eventSerializer.deserialize(body);
            this.replicasInfoManager.applyEvent(event);
        }
        firstApplyIndex = firstApplyIndex == -1 ? entry.getIndex() : firstApplyIndex;
        lastApplyIndex = entry.getIndex();
        applyingSize++;
    }
    log.info("Apply {} events index from {} to {} on controller {}",
        applyingSize, firstApplyIndex, lastApplyIndex, this.dLedgerId);
}
```

- [ ] **Step 2: Add an index-only committed-position cache**

Add imports for `DLedgerEntryCoder` and `DLedgerIndexEntry`, then add fields:

```java
private volatile long cachedCommittedIndex = Long.MIN_VALUE;
private volatile long cachedCommittedPos = -1;
```

Add this package-private method to `DLedgerCommitLog`:

```java
long getCommittedPos() {
    long committedIndex = dLedgerServer.getMemberState().getCommittedIndex();
    if (committedIndex < 0) {
        return -1;
    }
    if (committedIndex == cachedCommittedIndex) {
        return cachedCommittedPos;
    }

    SelectMmapBufferResult indexBuffer = null;
    try {
        indexBuffer = dLedgerFileStore.getIndexFileList().getData(
            committedIndex * DLedgerMmapFileStore.INDEX_UNIT_SIZE,
            DLedgerMmapFileStore.INDEX_UNIT_SIZE);
        if (indexBuffer == null) {
            return -1;
        }
        DLedgerIndexEntry indexEntry =
            DLedgerEntryCoder.decodeIndex(indexBuffer.getByteBuffer());
        if (indexEntry.getIndex() != committedIndex) {
            return -1;
        }
        long committedPos = indexEntry.getPosition() + indexEntry.getSize();
        cachedCommittedPos = committedPos;
        cachedCommittedIndex = committedIndex;
        return committedPos;
    } catch (RuntimeException e) {
        log.warn("Failed to resolve committed position for index={}", committedIndex, e);
        return -1;
    } finally {
        SelectMmapBufferResult.release(indexBuffer);
    }
}
```

Publishing `cachedCommittedIndex` last is intentional. Never substitute `ledgerEndIndex` or `dataFileList.getMaxWrotePosition()` when committed index is unknown; doing so would expose an uncommitted tail.

- [ ] **Step 3: Route every read boundary through one sampled committed position**

Update `getMaxOffset`, `truncate`, both `getData` overloads, and `getMessage(offset, size)`. Each method must call `getCommittedPos()` once and store the result in a local variable.

Use these guards:

```java
long committedPos = getCommittedPos();
if (committedPos > 0) {
    return committedPos;
}
```

```java
long committedPos = getCommittedPos();
if (sbr == null) {
    return null;
}
if (committedPos < 0 || sbr.getStartOffset() >= committedPos) {
    SelectMmapBufferResult.release(sbr);
    return null;
}
```

```java
long committedPos = getCommittedPos();
if (committedPos < 0 || offset >= committedPos) {
    return null;
}
```

For the fixed-size copy overload, reject a read crossing the boundary:

```java
long committedPos = getCommittedPos();
if (committedPos < 0 || offset >= committedPos || size > committedPos - offset) {
    return false;
}
```

Use this explicit start-and-end boundary in the `getMessage` override before it selects a DLedger mapped buffer:

```java
long committedPos = getCommittedPos();
if (committedPos < 0 || offset >= committedPos || size > committedPos - offset) {
    return null;
}
```

This guard is required even though the consume path normally uses `getData`: other `DefaultMessageStore` public read paths call `getMessage` directly, and the underlying DLedger data list contains appended-but-uncommitted bytes.

Do not enable fast advance yet; the restart test in Task 7 must first demonstrate why it is needed.

- [ ] **Step 4: Turn the compile RED green**

Run the consumer package command from the shared RocketMQ root:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-latest-dledger-package-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -Dfastjson2.version=2.0.64 \
  -pl store,controller,broker,container -am \
  -DskipTests package
```

Expected: all selected modules compile and package successfully against `0.3.3-pr336-f2-64-SNAPSHOT`.

- [ ] **Step 5: Smoke-test the controller consumer against PR #336**

The authoritative #336 RED/GREEN is Task 1, and Task 6 adds the RocketMQ store-consumer RED/GREEN. At this stage run a controller smoke test only against the PR artifact; do not rely on a controller scenario that may append late enough to hide the immediate-position bug:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
REPORT=controller/target/surefire-reports/TEST-org.apache.rocketmq.controller.impl.DLedgerControllerTest.xml
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-controller-pr336-green.start
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-controller-pr336-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl controller -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerControllerTest#testElectMaster' test
$ASSERT "$REPORT" 1 "$MARKER"
```

The selected method must pass. Run the full controller class only after Task 7 enables restart fast advance.

- [ ] **Step 6: Commit the dependency, compatibility, and mechanical API adaptation**

Before staging:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git diff --check
git status --short
git diff --name-only
```

Stage only the nine intended tracked files from Tasks 3–5, verify `broker/null/` is not staged, and commit:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git diff --check
git add \
  pom.xml \
  common/pom.xml \
  WORKSPACE \
  remoting/BUILD.bazel \
  remoting/src/test/java/org/apache/rocketmq/remoting/protocol/RemotingSerializableCompatTest.java \
  broker/src/main/java/org/apache/rocketmq/broker/dledger/DLedgerRoleChangeHandler.java \
  controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerController.java \
  controller/src/main/java/org/apache/rocketmq/controller/impl/DLedgerControllerStateMachine.java \
  store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java
git diff --cached --name-only
if git diff --cached --name-only | rg '^broker/null(?:/|$)'; then exit 1; fi
git commit -m "build: adapt RocketMQ to latest DLedger"
```

## Task 6: Add non-ignored store tests for commit boundaries and three-node failover

**Files:**

- Create: `store/src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest.java`
- Modify: `store/BUILD.bazel`

- [ ] **Step 1: Create a focused, executable test class**

Create `DLedgerLatestCommitLogTest extends MessageStoreTestBase`. It must have no `@Ignore` and no platform `Assume`. Add these helpers:

```java
private DLedgerCommitLog commitLog(DefaultMessageStore store) {
    return (DLedgerCommitLog) store.getCommitLog();
}

private void awaitLeader(DefaultMessageStore store) {
    await().atMost(Duration.ofSeconds(15)).until(
        () -> commitLog(store).getdLedgerServer().getMemberState().isLeader());
}

private void awaitQueue(DefaultMessageStore store, String topic, long maxOffset) {
    await().atMost(Duration.ofSeconds(15)).until(
        () -> store.getMaxOffsetInQueue(topic, 0) == maxOffset
            && store.dispatchBehindBytes() == 0);
}
```

Always set every test message's topic and queue ID explicitly to avoid `StoreTestBase`'s rotating queue IDs.

- [ ] **Step 2: Test that an uncommitted tail is not readable**

Build peers with `String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort())`, start only a forced leader `n0`, submit one message asynchronously, and wait for the quorum-timeout result. Assert:

```java
Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH,
    putResult.getPutMessageStatus());
Assert.assertEquals(-1, commitLog(store).getCommittedPos());
Assert.assertEquals(0, commitLog(store).getMaxOffset());
Assert.assertNull(commitLog(store).getData(0));
```

Also call the fixed-size `getData` overload and assert `false`. This test is the guard against replacing committed position with ledger end.

- [ ] **Step 3: Test single and batch appends on the latest API**

Start one electing node, wait for leadership, write one `MessageExtBrokerInner`, then a one-message `MessageExtBatch`, followed by a three-message `MessageExtBatch`. Assert all three `PutMessageStatus.PUT_OK`, batch `AppendMessageResult.getMsgNum()` values of 1 and 3, five logical messages in queue 0, strictly increasing physical offsets, non-null message IDs, and successful reads of queue offsets 0 through 4.

The exact batch setup is:

```java
MessageExtBatch oneMessageBatch = buildBatchMessage(1);
oneMessageBatch.setTopic(topic);
oneMessageBatch.setQueueId(0);
PutMessageResult oneMessageBatchResult = store.putMessages(oneMessageBatch);
Assert.assertEquals(PutMessageStatus.PUT_OK, oneMessageBatchResult.getPutMessageStatus());
Assert.assertEquals(1, oneMessageBatchResult.getAppendMessageResult().getLogicsOffset());
Assert.assertEquals(1, oneMessageBatchResult.getAppendMessageResult().getMsgNum());

MessageExtBatch batch = buildBatchMessage(3);
batch.setTopic(topic);
batch.setQueueId(0);
PutMessageResult batchResult = store.putMessages(batch);
Assert.assertEquals(PutMessageStatus.PUT_OK, batchResult.getPutMessageStatus());
Assert.assertEquals(2, batchResult.getAppendMessageResult().getLogicsOffset());
Assert.assertEquals(3, batchResult.getAppendMessageResult().getMsgNum());
```

- [ ] **Step 4: Test a real three-node store failover**

Allocate three DLedger ports and construct one peers string. Start `n0`, `n1`, and `n2` with separate base directories and leader election enabled. Wait until exactly one store reports leader. On that leader:

1. Put three single messages to topic/queue 0 and wait until all three stores report CQ max 3 with dispatch-behind 0.
2. Shut down the leader without destroying its data.
3. Wait for exactly one of the two live stores to become leader and read offsets 0 through 2 from it.
4. Call `recoverTopicQueueTable()` on that promoted store, exactly mirroring `DLedgerRoleChangeHandler` before a broker accepts writes.
5. Put a three-message batch through the promoted leader, starting at logical offset 3.
6. Wait until both live stores report CQ max 6, then read offsets 0 through 5 from the promoted leader.

Use `try/finally`; shut down every started store and let the inherited cleanup destroy only the test-created base directories.

The complete final test file for Steps 1–4 and Task 7 is below. Its independently compiled Corretto 8 prototype passed all eight methods after the production fix; this version additionally asserts that malformed NOOP parsing leaves the buffer position unchanged. Use the file exactly, then follow the RED/GREEN ordering in the surrounding steps rather than running all eight methods prematurely:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.common.ReadClosure;
import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.entry.DLedgerEntryType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class DLedgerLatestCommitLogTest extends MessageStoreTestBase {

    private static final int QUEUE_ID = 0;

    @Test
    public void testUncommittedTailIsNotReadable() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DefaultMessageStore leaderStore = null;
        try {
            leaderStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, "n0", false, 0);
            String topic = UUID.randomUUID().toString();
            MessageExtBrokerInner message = buildMessage();
            message.setTopic(topic);
            message.setQueueId(QUEUE_ID);

            PutMessageResult result = leaderStore.asyncPutMessage(message).get(5, SECONDS);
            Assert.assertEquals(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, result.getPutMessageStatus());
            Assert.assertNotNull(result.getAppendMessageResult());
            Assert.assertTrue(result.getAppendMessageResult().getWroteOffset() > 0);

            DLedgerCommitLog commitLog = commitLog(leaderStore);
            Assert.assertEquals(-1, commitLog.getdLedgerServer().getMemberState().getCommittedIndex());
            Assert.assertEquals(-1, commitLog.getCommittedPos());
            Assert.assertEquals(0, commitLog.getMaxOffset());
            Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, QUEUE_ID));
            Assert.assertNull(commitLog.getData(0));
            Assert.assertFalse(commitLog.getData(0, 1, ByteBuffer.allocate(1)));
            Assert.assertNull(commitLog.getMessage(
                result.getAppendMessageResult().getWroteOffset(), 1));
        } finally {
            shutdownAndDestroy(leaderStore);
        }
    }

    @Test
    public void testSingleAndBatchAppendPositions() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(messageStore));
            String topic = UUID.randomUUID().toString();

            PutMessageResult singleResult = putSingle(messageStore, topic, 0);
            PutMessageResult singleMessageBatchResult = putBatch(messageStore, topic, 1, 1);
            PutMessageResult batchResult = putBatch(messageStore, topic, 3, 2);

            Assert.assertTrue(singleResult.getAppendMessageResult().getWroteOffset() > 0);
            Assert.assertTrue(singleMessageBatchResult.getAppendMessageResult().getWroteOffset()
                > singleResult.getAppendMessageResult().getWroteOffset());
            Assert.assertTrue(batchResult.getAppendMessageResult().getWroteOffset()
                > singleMessageBatchResult.getAppendMessageResult().getWroteOffset());
            Assert.assertEquals(1, singleMessageBatchResult.getAppendMessageResult().getMsgNum());
            Assert.assertEquals(3, batchResult.getAppendMessageResult().getMsgNum());
            Assert.assertNotNull(singleResult.getAppendMessageResult().getMsgId());
            Assert.assertNotNull(singleMessageBatchResult.getAppendMessageResult().getMsgId());
            Assert.assertNotNull(batchResult.getAppendMessageResult().getMsgId());
            Assert.assertEquals(1,
                singleMessageBatchResult.getAppendMessageResult().getMsgId().split(",").length);
            Assert.assertEquals(3, batchResult.getAppendMessageResult().getMsgId().split(",").length);
            awaitStoreReady(messageStore, topic, 5);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertTrue(commitLog(messageStore).getCommittedPos()
                > batchResult.getAppendMessageResult().getWroteOffset());
            doGetMessages(messageStore, topic, QUEUE_ID, 5, 0);
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testThreeNodeElectionAndFailover() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d",
            nextPort(), nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        List<DefaultMessageStore> allStores = new ArrayList<>();
        try {
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n0", peers, null, false, 0));
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n1", peers, null, false, 0));
            allStores.add(createDledgerMessageStore(createBaseDir(), group, "n2", peers, null, false, 0));
            List<DefaultMessageStore> activeStores = new ArrayList<>(allStores);
            DefaultMessageStore firstLeader = awaitLeader(activeStores);
            String topic = UUID.randomUUID().toString();

            putSingle(firstLeader, topic, 0);
            putSingle(firstLeader, topic, 1);
            putSingle(firstLeader, topic, 2);
            for (DefaultMessageStore store : activeStores) {
                awaitStoreReady(store, topic, 3);
            }
            long committedBeforeFailover = commitLog(firstLeader).getCommittedPos();

            firstLeader.shutdown();
            activeStores.remove(firstLeader);
            DefaultMessageStore secondLeader = awaitLeader(activeStores);
            Assert.assertNotSame(firstLeader, secondLeader);
            awaitStoreReady(secondLeader, topic, 3);
            Assert.assertTrue(commitLog(secondLeader).getCommittedPos() >= committedBeforeFailover);
            doGetMessages(secondLeader, topic, QUEUE_ID, 3, 0);

            // Broker-side DLedgerRoleChangeHandler does this before accepting writes on a new leader.
            secondLeader.recoverTopicQueueTable();
            putBatch(secondLeader, topic, 3, 3);
            for (DefaultMessageStore store : activeStores) {
                awaitStoreReady(store, topic, 6);
            }
            doGetMessages(secondLeader, topic, QUEUE_ID, 6, 0);
        } finally {
            for (DefaultMessageStore store : allStores) {
                shutdownAndDestroy(store);
            }
        }
    }

    @Test
    public void testRestartRecoversCommittedBoundaryBeforeNewWrite() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        DefaultMessageStore currentStore = null;
        try {
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            doPutMessages(currentStore, topic, QUEUE_ID, 10, 0);
            awaitStoreReady(currentStore, topic, 10);
            doGetMessages(currentStore, topic, QUEUE_ID, 10, 0);
            long committedPosBeforeRestart = commitLog(currentStore).getCommittedPos();
            long committedIndexBeforeRestart = committedIndex(currentStore);
            Assert.assertTrue(committedPosBeforeRestart > 0);
            Assert.assertTrue(committedIndexBeforeRestart >= 9);

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeRestart);
            assertNoopEntry(currentStore, committedIndexBeforeRestart + 1);
            awaitStoreReady(currentStore, topic, 10);
            Assert.assertEquals(0, currentStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertTrue(commitLog(currentStore).getCommittedPos() >= committedPosBeforeRestart);
            Assert.assertEquals(commitLog(currentStore).getCommittedPos(), currentStore.getCommitLog().getMaxOffset());
            doGetMessages(currentStore, topic, QUEUE_ID, 10, 0);

            putSingle(currentStore, topic, 10);
            awaitStoreReady(currentStore, topic, 11);
            doGetMessages(currentStore, topic, QUEUE_ID, 11, 0);
            long committedPosBeforeSecondRestart = commitLog(currentStore).getCommittedPos();
            long committedIndexBeforeSecondRestart = committedIndex(currentStore);

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeSecondRestart);
            assertNoopEntry(currentStore, committedIndexBeforeSecondRestart + 1);
            awaitStoreReady(currentStore, topic, 11);
            Assert.assertEquals(0, currentStore.getMinOffsetInQueue(topic, QUEUE_ID));
            Assert.assertTrue(commitLog(currentStore).getCommittedPos() >= committedPosBeforeSecondRestart);
            Assert.assertEquals(commitLog(currentStore).getCommittedPos(), currentStore.getCommitLog().getMaxOffset());
            doGetMessages(currentStore, topic, QUEUE_ID, 11, 0);
        } finally {
            shutdownAndDestroy(currentStore);
        }
    }

    @Test
    public void testNoopDispatchContractAndBounds() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            DLedgerCommitLog commitLog = commitLog(messageStore);

            ByteBuffer noopBuffer = ByteBuffer.allocate(DLedgerEntry.BODY_OFFSET);
            DLedgerEntryCoder.encode(new DLedgerEntry(DLedgerEntryType.NOOP), noopBuffer);
            DispatchRequest noop = commitLog.checkMessageAndReturnSize(noopBuffer, true, false, false);
            Assert.assertTrue(noop.isSuccess());
            Assert.assertEquals(0, noop.getMsgSize());
            Assert.assertEquals(DLedgerEntry.BODY_OFFSET, noop.getBufferSize());
            Assert.assertEquals(DLedgerEntry.BODY_OFFSET, noopBuffer.position());

            ByteBuffer undersized = noopHeader(DLedgerEntry.BODY_OFFSET - 1);
            DispatchRequest invalidSize = commitLog.checkMessageAndReturnSize(undersized, true, false, false);
            Assert.assertFalse(invalidSize.isSuccess());
            Assert.assertEquals(-1, invalidSize.getMsgSize());
            Assert.assertEquals(0, undersized.position());

            ByteBuffer truncated = noopHeader(DLedgerEntry.BODY_OFFSET + 1);
            DispatchRequest invalidBounds = commitLog.checkMessageAndReturnSize(truncated, true, false, false);
            Assert.assertFalse(invalidBounds.isSuccess());
            Assert.assertEquals(-1, invalidBounds.getMsgSize());
            Assert.assertEquals(0, truncated.position());
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testRaftLogReadNoopDoesNotBuildConsumeQueue() throws Exception {
        String peers = String.format("n0-localhost:%d", nextPort());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = createDledgerMessageStore(
                createBaseDir(), UUID.randomUUID().toString(), "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(messageStore));
            Assert.assertTrue(messageStore.getConsumeQueueTable().isEmpty());
            long previousIndex = committedIndex(messageStore);

            Status status = appendRaftLogNoop(messageStore);
            Assert.assertTrue(status.isOk());
            awaitCommittedPast(messageStore, previousIndex);
            assertNoopEntry(messageStore, previousIndex + 1);
            awaitNoopConsumed(messageStore);

            Assert.assertTrue(messageStore.getConsumeQueueTable().isEmpty());
            Assert.assertTrue(commitLog(messageStore).getCommittedPos() > 0);
            Assert.assertEquals(commitLog(messageStore).getCommittedPos(), messageStore.getCommitLog().getMaxOffset());
        } finally {
            shutdownAndDestroy(messageStore);
        }
    }

    @Test
    public void testAbnormalRecoveryAcrossLeadingNoop() throws Exception {
        String base = createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        DefaultMessageStore currentStore = null;
        try {
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            awaitLeader(Arrays.asList(currentStore));
            Assert.assertTrue(appendRaftLogNoop(currentStore).isOk());
            awaitCommittedPast(currentStore, -1);
            assertNoopEntry(currentStore, 0);
            awaitNoopConsumed(currentStore);
            Assert.assertTrue(currentStore.getConsumeQueueTable().isEmpty());

            putSingle(currentStore, topic, 0);
            awaitStoreReady(currentStore, topic, 1);
            doGetMessages(currentStore, topic, QUEUE_ID, 1, 0);
            long committedIndexBeforeRestart = committedIndex(currentStore);
            long committedPosBeforeRestart = commitLog(currentStore).getCommittedPos();

            currentStore.shutdown();
            currentStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            awaitLeader(Arrays.asList(currentStore));
            awaitCommittedPast(currentStore, committedIndexBeforeRestart);
            assertNoopEntry(currentStore, 0);
            assertNoopEntry(currentStore, committedIndexBeforeRestart + 1);
            awaitStoreReady(currentStore, topic, 1);
            Assert.assertEquals(1, currentStore.getConsumeQueueTable().size());
            Assert.assertTrue(currentStore.getConsumeQueueTable().containsKey(topic));
            Assert.assertTrue(commitLog(currentStore).getCommittedPos() >= committedPosBeforeRestart);
            doGetMessages(currentStore, topic, QUEUE_ID, 1, 0);

            putSingle(currentStore, topic, 1);
            awaitStoreReady(currentStore, topic, 2);
            doGetMessages(currentStore, topic, QUEUE_ID, 2, 0);
        } finally {
            shutdownAndDestroy(currentStore);
        }
    }

    private PutMessageResult putSingle(DefaultMessageStore messageStore, String topic, long expectedLogicOffset)
        throws Exception {
        MessageExtBrokerInner message = buildMessage();
        message.setTopic(topic);
        message.setQueueId(QUEUE_ID);
        PutMessageResult result = messageStore.asyncPutMessage(message).get(5, SECONDS);
        Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        Assert.assertNotNull(result.getAppendMessageResult());
        Assert.assertEquals(expectedLogicOffset, result.getAppendMessageResult().getLogicsOffset());
        return result;
    }

    private PutMessageResult putBatch(DefaultMessageStore messageStore, String topic, int batchSize,
        long expectedLogicOffset) throws Exception {
        MessageExtBatch batch = buildBatchMessage(batchSize);
        batch.setTopic(topic);
        batch.setQueueId(QUEUE_ID);
        PutMessageResult result = messageStore.asyncPutMessages(batch).get(5, SECONDS);
        Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        Assert.assertNotNull(result.getAppendMessageResult());
        Assert.assertEquals(expectedLogicOffset, result.getAppendMessageResult().getLogicsOffset());
        return result;
    }

    private DefaultMessageStore awaitLeader(List<DefaultMessageStore> stores) {
        AtomicReference<DefaultMessageStore> leaderRef = new AtomicReference<>();
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
            DefaultMessageStore leader = null;
            for (DefaultMessageStore store : stores) {
                if (commitLog(store).getdLedgerServer().getMemberState().isLeader()) {
                    if (leader != null) {
                        return false;
                    }
                    leader = store;
                }
            }
            leaderRef.set(leader);
            return leader != null;
        });
        return leaderRef.get();
    }

    private void awaitStoreReady(DefaultMessageStore messageStore, String topic, long expectedMaxOffset) {
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(expectedMaxOffset, messageStore.getMaxOffsetInQueue(topic, QUEUE_ID));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        });
    }

    private void awaitCommittedPast(DefaultMessageStore messageStore, long previousCommittedIndex) {
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS)
            .until(() -> committedIndex(messageStore) > previousCommittedIndex);
    }

    private void awaitNoopConsumed(DefaultMessageStore messageStore) {
        await().atMost(15, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(commitLog(messageStore).getCommittedPos(), messageStore.getCommitLog().getMaxOffset());
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        });
    }

    private Status appendRaftLogNoop(DefaultMessageStore messageStore) throws Exception {
        CompletableFuture<Status> result = new CompletableFuture<>();
        commitLog(messageStore).getdLedgerServer().handleRead(ReadMode.RAFT_LOG_READ, new ReadClosure() {
            @Override
            public void done(Status status) {
                result.complete(status);
            }
        });
        return result.get(5, SECONDS);
    }

    private ByteBuffer noopHeader(int entrySize) {
        ByteBuffer buffer = ByteBuffer.allocate(DLedgerEntry.BODY_OFFSET);
        buffer.putInt(DLedgerEntryType.NOOP.getMagic());
        buffer.putInt(entrySize);
        buffer.position(0);
        buffer.limit(DLedgerEntry.BODY_OFFSET);
        return buffer;
    }

    private void assertNoopEntry(DefaultMessageStore messageStore, long index) {
        DLedgerServer server = commitLog(messageStore).getdLedgerServer();
        DLedgerEntry entry = server.getDLedgerStore().get(index);
        Assert.assertNotNull(entry);
        Assert.assertEquals(DLedgerEntryType.NOOP.getMagic(), entry.getMagic());
    }

    private long committedIndex(DefaultMessageStore messageStore) {
        return commitLog(messageStore).getdLedgerServer().getMemberState().getCommittedIndex();
    }

    private DLedgerCommitLog commitLog(DefaultMessageStore messageStore) {
        return (DLedgerCommitLog) messageStore.getCommitLog();
    }

    private void shutdownAndDestroy(DefaultMessageStore messageStore) {
        if (messageStore == null) {
            return;
        }
        try {
            if (!messageStore.isShutdown()) {
                messageStore.shutdown();
            }
        } finally {
            messageStore.destroy();
        }
    }
}
```

- [ ] **Step 5: Add the new test to Bazel's medium size list**

In `store/BUILD.bazel` add:

```starlark
"src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest",
```

The generated target will be:

```text
//store:src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest
```

- [ ] **Step 6: Run the store tests against master and PR #336**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-master-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-master-f2-64-SNAPSHOT \
  -pl store -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testSingleAndBatchAppendPositions' test
```

Expected: the master artifact fails with an immediate-position symptom such as `OS_PAGE_CACHE_BUSY` or an unknown append result. Then run only the three pre-restart methods with the PR artifact; the five remaining restart, NOOP, recovery, and committed-boundary methods—for eight methods total—are intentionally left for their RED gates in Task 7:

```bash
set -euo pipefail
LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-master-red.log
rg -F 'OS_PAGE_CACHE_BUSY' "$LOG"
if rg 'DISK_FULL|BindException|Address already in use|Could not resolve dependencies' "$LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
REPORT=store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerLatestCommitLogTest.xml
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-uncommitted-green.start
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-uncommitted-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testUncommittedTailIsNotReadable' test
$ASSERT "$REPORT" 1 "$MARKER"

MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-single-batch-green.start
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-single-batch-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testSingleAndBatchAppendPositions' test
$ASSERT "$REPORT" 1 "$MARKER"

MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-three-node-green.start
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-pr336-three-node-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testThreeNodeElectionAndFailover' test
$ASSERT "$REPORT" 1 "$MARKER"
```

At this point the uncommitted-tail, single/batch, and three-node methods must pass. The prewritten restart and NOOP methods run next so each can produce a distinct RED before its production fix.

## Task 7: Reproduce and fix NOOP framing and existing-data restart before the first user write

**Files:**

- Modify: `store/src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest.java`
- Modify: `store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java`
- Modify: `store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java`
- Modify: `controller/src/test/java/org/apache/rocketmq/controller/impl/DLedgerControllerTest.java`

- [ ] **Step 1: Run the prewritten restart regression before enabling fast advance**

The complete class from Task 6 already contains `testRestartRecoversCommittedBoundaryBeforeNewWrite` with this exact sequence:

1. Create one electing DLedger store using one base directory, group, self ID, and peers string.
2. Wait for leader.
3. Write ten messages to one topic/queue, wait for CQ max 10 and dispatch-behind 0, read all ten, and record `committedBeforeRestart = commitLog.getCommittedPos()` and `physicalBeforeRestart = store.getMaxPhyOffset()`.
4. Shut down without destroy.
5. Recreate a store with the same base/group/self/peers.
6. Before any user put, wait at most 15 seconds for all of these conditions:

```java
commitLog(restarted).getdLedgerServer().getMemberState().isLeader()
    && commitLog(restarted).getdLedgerServer().getMemberState().getCommittedIndex()
        == commitLog(restarted).getdLedgerServer().getDLedgerStore().getLedgerEndIndex()
    && restarted.dispatchBehindBytes() == 0
```

7. Still before any put, assert:

```java
Assert.assertTrue(commitLog(restarted).getCommittedPos() >= committedBeforeRestart);
Assert.assertTrue(restarted.getMaxPhyOffset() >= physicalBeforeRestart);
Assert.assertEquals(0, restarted.getMinOffsetInQueue(topic, 0));
Assert.assertEquals(10, restarted.getMaxOffsetInQueue(topic, 0));
doGetMessages(restarted, topic, 0, 10, 0);
```

8. Put message 11, assert logical offset 10, wait for CQ max 11, and read it.
9. Shut down and recreate a second time; before another write, verify all eleven messages again.

The physical maximum uses `>=`, not equality: safe fast advance appends a 48-byte DLedger NOOP on a new leader. CQ max and user-message contents must remain unchanged by that NOOP.

- [ ] **Step 2: Capture the restart RED**

Run only the new method with the PR #336 artifact and no fast-advance setting in production code:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-restart-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testRestartRecoversCommittedBoundaryBeforeNewWrite' \
  test
```

Expected: the restarted leader retains `committedIndex == -1` or never converges to ledger end before a new user append. A disk-full or port failure is not the required RED.

```bash
set -euo pipefail
LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-restart-red.log
rg -n 'ConditionTimeoutException|committedIndex[^0-9-]*-1' "$LOG"
if rg 'DISK_FULL|BindException|Address already in use|Could not resolve dependencies' "$LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

- [ ] **Step 3: Add a deterministic controller replay characterization test**

Add `testRestartAllControllersRecoversStateBeforeNewEvent` to `DLedgerControllerTest`:

```java
@Test
public void testRestartAllControllersRecoversStateBeforeNewEvent() throws Exception {
    DLedgerController originalLeader = mockMetaData(false);
    String group = originalLeader.getControllerConfig().getControllerDLegerGroup();
    String peers = originalLeader.getControllerConfig().getControllerDLegerPeers();
    List<String> selfIds = controllers.stream()
        .map(controller -> controller.getControllerConfig().getControllerDLegerSelfId())
        .collect(Collectors.toList());

    for (DLedgerController controller : new ArrayList<>(controllers)) {
        controller.shutdown();
    }
    controllers.clear();

    for (String selfId : selfIds) {
        controllers.add(launchController(group, peers, selfId, false));
    }
    DLedgerController restartedLeader = waitLeader(controllers);

    RemotingCommand response = restartedLeader.getReplicaInfo(
        new GetReplicaInfoRequestHeader(DEFAULT_BROKER_NAME)).get(10, TimeUnit.SECONDS);
    assertEquals(ResponseCode.SUCCESS, response.getCode());
    GetReplicaInfoResponseHeader replicaInfo =
        (GetReplicaInfoResponseHeader) response.readCustomHeader();
    SyncStateSet syncStateSet =
        RemotingSerializable.decode(response.getBody(), SyncStateSet.class);
    assertEquals(1L, replicaInfo.getMasterBrokerId().longValue());
    assertEquals(DEFAULT_IP[0], replicaInfo.getMasterAddress());
    assertEquals(new HashSet<>(Arrays.asList(1L, 2L, 3L)),
        syncStateSet.getSyncStateSet());
}
```

Add `java.util.Arrays`. The first request after restart is read-only; there must be no new DLedger mutation before these assertions.

Run only this method against PR #336 before changing the store configuration:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
REPORT=controller/target/surefire-reports/TEST-org.apache.rocketmq.controller.impl.DLedgerControllerTest.xml
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-controller-restart-green.start
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-controller-restart-green.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl controller -am \
  -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerControllerTest#testRestartAllControllersRecoversStateBeforeNewEvent' test
$ASSERT "$REPORT" 1 "$MARKER"
```

Expected: GREEN before the store fix. `DLedgerController.RoleChangeHandler` already appends and waits for an empty NORMAL proposal before setting `isLeaderState`; that existing mechanism commits preceding entries and replays controller state. This test protects that behavior and is not evidence for adding fast advance to the controller.

- [ ] **Step 4: Run the prewritten deterministic RED tests for DLedger's internal NOOP framing**

The complete Task 6 class already contains these methods; run them before changing production code:

1. `testNoopDispatchContractAndBounds` creates a 48-byte data entry with `DLedgerEntryType.NOOP.getMagic()`, declared size `DLedgerEntry.BODY_OFFSET`, zeroed index/term/position/channel/CRCs/body length, and calls `checkMessageAndReturnSize`. The final contract is success, `msgSize == 0`, `bufferSize == 48`, and `buffer.position() == 48`. A 48-byte buffer declaring an undersized entry of 47 bytes and a 48-byte buffer declaring an oversized entry of 49 bytes must both fail without moving beyond their limit.
2. `testRaftLogReadNoopDoesNotBuildConsumeQueue` starts a one-node store with an empty CQ table and appends a real internal entry with `DLedgerServer#handleRead(ReadMode.RAFT_LOG_READ, ReadClosure)`. After its `Status` is OK, require ledger end to increase by one, the CQ table to remain empty, `dispatchBehindBytes() == 0`, and committed/max physical boundaries to agree.
3. `testAbnormalRecoveryAcrossLeadingNoop` creates a real NOOP as index zero, writes one normal message, shuts down without destroying data, creates the abort marker so reopening must take the abnormal-replay path, then—before a user put—requires CQ max one, the old body readable, and dispatch behind zero. Finally append/read the second user message at logical offset one.

Use the latest DLedger imports `io.openmessaging.storage.dledger.common.ReadClosure`, `ReadMode`, and `Status`. Run the parser and real-NOOP tests separately so each RED is attributable:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-noop-parser-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testNoopDispatchContractAndBounds' test
```

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-noop-append-red.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerLatestCommitLogTest#testRaftLogReadNoopDoesNotBuildConsumeQueue' test
```

Expected: the parser test fails because the empty body is handed to RocketMQ's message decoder; the real-NOOP test fails at the unconditional append hook or tries to dispatch an empty logical message. Neither failure may be a timeout caused by disk or ports.

```bash
set -euo pipefail
PARSER_LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-noop-parser-red.log
APPEND_LOG=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-store-noop-append-red.log
rg -n 'testNoopDispatchContractAndBounds.*(FAILURE|ERROR)|expected:.*but was:' "$PARSER_LOG"
rg -n 'IndexOutOfBoundsException|BufferOverflowException|testRaftLogReadNoopDoesNotBuildConsumeQueue.*(FAILURE|ERROR)' "$APPEND_LOG"
if rg 'DISK_FULL|BindException|Address already in use|Could not resolve dependencies' \
  "$PARSER_LOG" "$APPEND_LOG"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

- [ ] **Step 5: Make DLedger NOOP a physical-only entry in append, parse, reput, and recovery**

Import `DLedgerEntryType`. First, guard the append hook so only a NORMAL entry is patched with RocketMQ's physical message offset:

```java
DLedgerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
    if (entry.getMagic() != DLedgerEntryType.NORMAL.getMagic()) {
        return;
    }
    assert bodyOffset == DLedgerEntry.BODY_OFFSET;
    buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
    buffer.putLong(entry.getPos() + bodyOffset);
};
```

Replace `DLedgerCommitLog#checkMessageAndReturnSize` with this exact implementation. It preserves old-commitlog detection, reads the outer header with absolute offsets so a truncated header does not move the source buffer, and makes the outer declared size the physical advancement contract:

```java
@Override
public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC,
    final boolean checkDupInfo, final boolean readBody) {
    if (isInrecoveringOldCommitlog) {
        return super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
    }
    try {
        int pos = byteBuffer.position();
        if (byteBuffer.remaining() < 2 * Integer.BYTES) {
            return new DispatchRequest(-1, false);
        }
        int magic = byteBuffer.getInt(pos);
        int entrySize = byteBuffer.getInt(pos + Integer.BYTES);
        if (entrySize == CommitLog.BLANK_MAGIC_CODE
            || entrySize == MessageDecoder.MESSAGE_MAGIC_CODE
            || entrySize == MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
        }
        if (magic == MmapFileList.BLANK_MAGIC_CODE) {
            return new DispatchRequest(0, true);
        }
        if (magic == DLedgerEntryType.NOOP.getMagic()) {
            if (entrySize != DLedgerEntry.BODY_OFFSET || entrySize > byteBuffer.remaining()) {
                return new DispatchRequest(-1, false);
            }
            byteBuffer.position(pos + entrySize);
            DispatchRequest request = new DispatchRequest(0, true);
            request.setBufferSize(entrySize);
            return request;
        }
        if (magic != DLedgerEntryType.NORMAL.getMagic()
            || entrySize < DLedgerEntry.BODY_OFFSET
            || entrySize > byteBuffer.remaining()) {
            return new DispatchRequest(-1, false);
        }
        byteBuffer.position(pos + DLedgerEntry.BODY_OFFSET);
        DispatchRequest request =
            super.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
        if (request.isSuccess()) {
            if (request.getMsgSize() + DLedgerEntry.BODY_OFFSET != entrySize) {
                return new DispatchRequest(-1, false);
            }
            request.setBufferSize(entrySize);
        } else if (request.getMsgSize() > 0) {
            request.setBufferSize(request.getMsgSize() + DLedgerEntry.BODY_OFFSET);
        }
        return request;
    } catch (Throwable ignored) {
        return new DispatchRequest(-1, false);
    }
}
```

The data header is authoritative: do not inspect DLedger index magic, because latest `DLedgerMmapFileStore#appendAsLeader` still writes NORMAL into index metadata for an internal NOOP.

In `DefaultMessageStore.ReputMessageService#doReput`, keep physical advancement separate from logical dispatch:

```java
if (size > 0) {
    if (dispatchRequest.getMsgSize() > 0) {
        currentReputTimestamp = dispatchRequest.getStoreTimestamp();
        DefaultMessageStore.this.doDispatch(dispatchRequest);
        if (isNotifyMessageArriveWhenReput()) {
            notifyMessageArriveIfNecessary(dispatchRequest);
        }
        if (!DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
            && DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                == BrokerRole.SLAVE) {
            DefaultMessageStore.this.storeStatsService
                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic())
                .add(dispatchRequest.getBatchSize());
            DefaultMessageStore.this.storeStatsService
                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                .add(dispatchRequest.getMsgSize());
        }
    }
    this.reputFromOffset += size;
    readSize += size;
} else {
    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
    readSize = result.getSize();
}
```

For a NOOP, `msgSize == 0` prevents CQ/index/notify/stat dispatch while `bufferSize == 48` makes forward progress. Only `msgSize == 0 && bufferSize == 0` means a mapped-file tail.

In `DLedgerCommitLog#dledgerRecoverAbnormally`, use the outer entry size for physical advancement and the inner message size for dispatch:

```java
int messageSize = dispatchRequest.getMsgSize();
int entrySize = dispatchRequest.getBufferSize() == -1
    ? messageSize : dispatchRequest.getBufferSize();
if (dispatchRequest.isSuccess()) {
    if (entrySize > 0) {
        mmapFileOffset += entrySize;
        if (messageSize > 0) {
            if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                if (dispatchRequest.getCommitLogOffset()
                    < this.defaultMessageStore.getConfirmOffset()) {
                    this.defaultMessageStore.doDispatch(dispatchRequest);
                }
            } else {
                this.defaultMessageStore.doDispatch(dispatchRequest);
            }
        }
    } else {
        index++;
        if (index >= mmapFiles.size()) {
            log.info("dledger recover physics file over, last mapped file {}",
                mmapFile.getFileName());
            break;
        }
        mmapFile = mmapFiles.get(index);
        byteBuffer = mmapFile.sliceByteBuffer();
        processOffset = mmapFile.getFileFromOffset();
        mmapFileOffset = 0;
        log.info("dledger recover next physics file, {}", mmapFile.getFileName());
    }
} else {
    log.info("dledger recover physics file end, {} pos={}",
        mmapFile.getFileName(), byteBuffer.position());
    break;
}
```

Finally, make abnormal-recovery file selection skip leading internal entries instead of reading RocketMQ fields at byte 48. Add this helper; its sliced limit prevents a malformed RocketMQ header from reading into the next DLedger entry:

```java
private ByteBuffer firstNormalEntryBody(ByteBuffer source) {
    int entryPos = source.position();
    while (source.limit() - entryPos >= DLedgerEntry.BODY_OFFSET) {
        int magic = source.getInt(entryPos);
        int entrySize = source.getInt(entryPos + Integer.BYTES);
        if (magic == MmapFileList.BLANK_MAGIC_CODE) {
            return null;
        }
        if (entrySize < DLedgerEntry.BODY_OFFSET
            || entrySize > source.limit() - entryPos) {
            return null;
        }
        if (magic == DLedgerEntryType.NORMAL.getMagic()) {
            ByteBuffer body = source.duplicate();
            body.position(entryPos + DLedgerEntry.BODY_OFFSET);
            body.limit(entryPos + entrySize);
            return body.slice();
        }
        if (magic != DLedgerEntryType.NOOP.getMagic()) {
            return null;
        }
        entryPos += entrySize;
    }
    return null;
}
```

Start `isMmapFileMatchedRecover` with this exact bounded read, then retain its existing checkpoint and consume-queue comparison after `phyOffset` is computed:

```java
ByteBuffer body = firstNormalEntryBody(mmapFile.sliceByteBuffer());
if (body == null
    || body.limit() < MessageDecoder.MESSAGE_MAGIC_CODE_POSITION + Integer.BYTES
    || body.limit() < MessageDecoder.SYSFLAG_POSITION + Integer.BYTES) {
    return false;
}
int magicCode = body.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE
    && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
    return false;
}
int sysFlag = body.getInt(MessageDecoder.SYSFLAG_POSITION);
int storeTimestampPosition = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION;
if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) != 0) {
    storeTimestampPosition += 12;
}
if (body.limit() < storeTimestampPosition + Long.BYTES
    || body.limit() < MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION + Long.BYTES) {
    return false;
}
long storeTimestamp = body.getLong(storeTimestampPosition);
long phyOffset = body.getLong(MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
```

Do not modify DLedger PR #336, DLedger's index store/coder, generic `CommitLog` recovery, consume-queue dispatchers, or the controller. `enableBuildConsumeQueueConcurrently` already cannot parse NORMAL DLedger framing in its pre-check path; keep that pre-existing non-default combination out of this patch and state it explicitly in the final limitations.

Run the two former RED methods while fast advance is still disabled. The abnormal-recovery test intentionally expects a restart-generated NOOP and therefore runs only after Step 6 enables fast advance:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
RUN=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged
JDK8=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8
LOGS=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs
M2=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2
TMP=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp
REPORT=store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerLatestCommitLogTest.xml
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
MVN_ARGS=(-B -ntp -nsu -Dmaven.repo.local="$M2" -Djava.io.tmpdir="$TMP" \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT -pl store -am \
  -Dsurefire.failIfNoSpecifiedTests=false)

MARKER="$LOGS/rocketmq-store-noop-parser-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-store-noop-parser-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  '-Dtest=DLedgerLatestCommitLogTest#testNoopDispatchContractAndBounds' test
$ASSERT "$REPORT" 1 "$MARKER"
MARKER="$LOGS/rocketmq-store-noop-append-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-store-noop-append-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  '-Dtest=DLedgerLatestCommitLogTest#testRaftLogReadNoopDoesNotBuildConsumeQueue' test
$ASSERT "$REPORT" 1 "$MARKER"
```

Both commands must pass.

- [ ] **Step 6: Enable DLedger's safe commit-index fast advance in the message-store consumer**

In the `DLedgerCommitLog` constructor, before creating `DLedgerServer`, add directly after batch append configuration:

```java
dLedgerConfig.setEnableBatchAppend(
    defaultMessageStore.getMessageStoreConfig().isEnableBatchPush());
dLedgerConfig.setEnableFastAdvanceCommitIndex(true);
```

Do not add a RocketMQ-side NOOP, do not restore a removed checkpoint, and do not treat ledger end as committed.

- [ ] **Step 7: Turn the store restart path green and recheck controller replay**

Run the restart method, the full store class, the focused controller characterization, and the full controller class:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
RUN=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged
JDK8=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8
LOGS=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs
M2=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2
TMP=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp
STORE_REPORT=store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerLatestCommitLogTest.xml
CONTROLLER_REPORT=controller/target/surefire-reports/TEST-org.apache.rocketmq.controller.impl.DLedgerControllerTest.xml
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
MVN_ARGS=(-B -ntp -nsu -Dmaven.repo.local="$M2" -Djava.io.tmpdir="$TMP" \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -Dsurefire.failIfNoSpecifiedTests=false)

MARKER="$LOGS/rocketmq-store-restart-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-store-restart-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  -pl store -am \
  '-Dtest=DLedgerLatestCommitLogTest#testRestartRecoversCommittedBoundaryBeforeNewWrite' test
$ASSERT "$STORE_REPORT" 1 "$MARKER"
MARKER="$LOGS/rocketmq-store-noop-abnormal-recovery-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-store-noop-abnormal-recovery-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  -pl store -am \
  '-Dtest=DLedgerLatestCommitLogTest#testAbnormalRecoveryAcrossLeadingNoop' test
$ASSERT "$STORE_REPORT" 1 "$MARKER"
MARKER="$LOGS/rocketmq-store-full-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-store-full-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  -pl store -am -Dtest=DLedgerLatestCommitLogTest test
$ASSERT "$STORE_REPORT" 8 "$MARKER"
MARKER="$LOGS/rocketmq-controller-restart-after-store-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-controller-restart-after-store-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  -pl controller -am \
  '-Dtest=DLedgerControllerTest#testRestartAllControllersRecoversStateBeforeNewEvent' test
$ASSERT "$CONTROLLER_REPORT" 1 "$MARKER"
MARKER="$LOGS/rocketmq-controller-full-green.start"
touch "$MARKER"
$RUN "$LOGS/rocketmq-controller-full-green.log" $JDK8 mvn "${MVN_ARGS[@]}" \
  -pl controller -am -Dtest=DLedgerControllerTest test
$ASSERT "$CONTROLLER_REPORT" 6 "$MARKER"
```

Expected: store leadership converges, committed index equals ledger end, NOOP advances the physical/reput boundary without changing CQ offsets, old messages are readable before a user write, all eight store methods pass including abnormal recovery and the second restart, controller metadata remains available before a new mutation, and all six controller methods pass after controller replay/leader change.

- [ ] **Step 8: Commit the restart/failover behavior**

Run `git diff --check`, stage the exact store production/test/BUILD files plus the controller characterization test, then commit:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git diff --check
git add \
  store/src/main/java/org/apache/rocketmq/store/dledger/DLedgerCommitLog.java \
  store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java \
  store/src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest.java \
  store/BUILD.bazel \
  controller/src/test/java/org/apache/rocketmq/controller/impl/DLedgerControllerTest.java
git diff --cached --name-only
if git diff --cached --name-only | rg '^broker/null(?:/|$)'; then exit 1; fi
git commit -m "fix: handle DLedger control entries during recovery"
```

## Task 8: Add a real three-broker DLedger failover and full-restart IT

**Files:**

- Create: `test/src/test/java/org/apache/rocketmq/test/dledger/DLedgerThreeNodeIT.java`

- [ ] **Step 1: Create the complete three-node integration test**

Create the complete file below. It deliberately owns its NameServer and brokers instead of loading `BaseConf`, which would start three unrelated default brokers.

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.test.dledger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class DLedgerThreeNodeIT {
    private static final long AWAIT_SECONDS = 60;
    private static final List<String> NODE_IDS = Arrays.asList("n0", "n1", "n2");

    @Test
    public void testProduceFailoverAndRestart() throws Exception {
        NamesrvController namesrvController = null;
        DefaultMQAdminExt admin = null;
        List<NodeSpec> nodeSpecs = new ArrayList<>();
        List<BrokerController> allControllers = new ArrayList<>();
        Set<BrokerController> stopped =
            Collections.newSetFromMap(new IdentityHashMap<BrokerController, Boolean>());
        try {
            namesrvController = IntegrationTestBase.createAndStartNamesrv();
            String namesrvAddr = "127.0.0.1:"
                + namesrvController.getNettyServerConfig().getListenPort();
            admin = new DefaultMQAdminExt();
            admin.setInstanceName(UUID.randomUUID().toString());
            admin.setNamesrvAddr(namesrvAddr);
            admin.start();

            String clusterName = "DLedgerCluster-" + UUID.randomUUID();
            String brokerName = "DLedgerBroker-" + UUID.randomUUID();
            String topic = "DLedgerTopic-" + UUID.randomUUID();
            List<Integer> dLedgerPorts = allocatePorts(NODE_IDS.size());
            String peers = buildPeers(dLedgerPorts);
            for (int i = 0; i < NODE_IDS.size(); i++) {
                nodeSpecs.add(new NodeSpec(
                    NODE_IDS.get(i), dLedgerPorts.get(i), IntegrationTestBase.createBaseDir()));
            }

            List<BrokerController> active = startCluster(
                nodeSpecs, clusterName, brokerName, namesrvAddr, peers, allControllers);
            BrokerController initialLeader = awaitLeader(active);
            awaitClusterMaster(admin, brokerName, initialLeader);
            Assert.assertTrue(IntegrationTestBase.initTopic(
                topic, namesrvAddr, clusterName, 1, CQType.SimpleCQ));
            awaitTopicRouteMaster(admin, topic, brokerName, initialLeader);
            awaitTopicOnEveryNode(active, topic);

            List<String> expectedBodies = new ArrayList<>();
            expectedBodies.add("before-single");
            expectedBodies.add("before-batch-0");
            expectedBodies.add("before-batch-1");
            expectedBodies.add("before-batch-2");
            sendInitialSingleAndBatch(namesrvAddr, topic, brokerName);
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);

            stopController(initialLeader, stopped);
            active.remove(initialLeader);
            BrokerController failoverLeader = awaitLeader(active);
            Assert.assertNotSame(initialLeader, failoverLeader);
            awaitClusterMaster(admin, brokerName, failoverLeader);
            awaitTopicRouteMaster(admin, topic, brokerName, failoverLeader);
            sendOne(namesrvAddr, topic, brokerName,
                "after-failover", expectedBodies.size());
            expectedBodies.add("after-failover");
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);

            stopControllers(active, stopped);
            awaitDLedgerPortsAvailable(nodeSpecs);
            active = startCluster(
                nodeSpecs, clusterName, brokerName, namesrvAddr, peers, allControllers);
            BrokerController restartedLeader = awaitLeader(active);
            awaitClusterMaster(admin, brokerName, restartedLeader);
            awaitTopicRouteMaster(admin, topic, brokerName, restartedLeader);
            awaitTopicOnEveryNode(active, topic);
            awaitQueueOffset(active, topic, expectedBodies.size());

            // This pull is deliberately before the first post-restart user append.
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);
            sendOne(namesrvAddr, topic, brokerName,
                "after-restart", expectedBodies.size());
            expectedBodies.add("after-restart");
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);
        } finally {
            stopControllers(allControllers, stopped);
            if (admin != null) {
                admin.shutdown();
            }
            if (namesrvController != null) {
                namesrvController.shutdown();
            }
            for (NodeSpec nodeSpec : nodeSpecs) {
                UtilAll.deleteFile(new File(nodeSpec.storeRoot));
            }
        }
    }

    private static List<BrokerController> startCluster(List<NodeSpec> nodeSpecs,
        String clusterName, String brokerName, String namesrvAddr, String peers,
        List<BrokerController> allControllers) throws Exception {
        List<BrokerController> controllers = new ArrayList<>();
        for (NodeSpec nodeSpec : nodeSpecs) {
            BrokerController controller = startNode(
                nodeSpec, clusterName, brokerName, namesrvAddr, peers);
            controllers.add(controller);
            allControllers.add(controller);
        }
        return controllers;
    }

    private static BrokerController startNode(NodeSpec nodeSpec, String clusterName,
        String brokerName, String namesrvAddr, String peers) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerClusterName(clusterName);
        brokerConfig.setBrokerName(brokerName);
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setBrokerIP2("127.0.0.1");
        brokerConfig.setNamesrvAddr(namesrvAddr);
        brokerConfig.setRegisterNameServerPeriod(1000);
        brokerConfig.setLoadBalancePollNameServerInterval(500);

        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(nodeSpec.storeRoot);
        storeConfig.setStorePathCommitLog(
            nodeSpec.storeRoot + File.separator + "commitlog");
        storeConfig.setStorePathDLedgerCommitLog(
            nodeSpec.storeRoot + File.separator + "dledger");
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024);
        storeConfig.setMaxHashSlotNum(10_000);
        storeConfig.setMaxIndexNum(10_000);
        storeConfig.setHaListenPort(0);
        storeConfig.setEnableDLegerCommitLog(true);
        storeConfig.setdLegerGroup(brokerName);
        storeConfig.setdLegerSelfId(nodeSpec.selfId);
        storeConfig.setdLegerPeers(peers);
        storeConfig.setEnableBatchPush(true);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(0);
        BrokerController controller = new BrokerController(
            brokerConfig, serverConfig, new NettyClientConfig(), storeConfig);
        try {
            Assert.assertTrue(controller.initialize());
            controller.start();
            return controller;
        } catch (Throwable t) {
            try {
                controller.shutdown();
            } catch (Throwable ignored) {
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new RuntimeException(t);
        }
    }

    private static BrokerController awaitLeader(List<BrokerController> controllers) {
        AtomicReference<BrokerController> result = new AtomicReference<>();
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                BrokerController leader = findLeader(controllers);
                if (leader == null) {
                    return false;
                }
                result.set(leader);
                return true;
            });
        return result.get();
    }

    private static BrokerController findLeader(List<BrokerController> controllers) {
        BrokerController result = null;
        for (BrokerController controller : controllers) {
            DLedgerCommitLog commitLog =
                (DLedgerCommitLog) controller.getMessageStore().getCommitLog();
            boolean dLedgerLeader =
                commitLog.getdLedgerServer().getMemberState().isLeader();
            boolean brokerMaster = controller.getMessageStoreConfig().getBrokerRole()
                == BrokerRole.SYNC_MASTER;
            boolean brokerIdIsMaster =
                controller.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID;
            if (dLedgerLeader && brokerMaster && brokerIdIsMaster) {
                if (result != null) {
                    return null;
                }
                result = controller;
            }
        }
        return result;
    }

    private static void awaitClusterMaster(DefaultMQAdminExt admin, String brokerName,
        BrokerController expectedLeader) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
                    BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                    if (brokerData == null) {
                        return false;
                    }
                    return expectedLeader.getBrokerAddr().equals(
                        brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
                } catch (Exception ignored) {
                    return false;
                }
            });
    }

    private static void awaitTopicRouteMaster(DefaultMQAdminExt admin, String topic,
        String brokerName, BrokerController expectedLeader) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    TopicRouteData route = admin.examineTopicRouteInfo(topic);
                    for (BrokerData brokerData : route.getBrokerDatas()) {
                        if (brokerName.equals(brokerData.getBrokerName())) {
                            return expectedLeader.getBrokerAddr().equals(
                                brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
                        }
                    }
                } catch (Exception ignored) {
                }
                return false;
            });
    }

    private static void awaitTopicOnEveryNode(
        List<BrokerController> controllers, String topic) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                for (BrokerController controller : controllers) {
                    if (controller.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        return false;
                    }
                }
                return true;
            });
    }

    private static void awaitQueueOffset(List<BrokerController> controllers,
        String topic, long expectedOffset) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                for (BrokerController controller : controllers) {
                    if (controller.getMessageStore().getMaxOffsetInQueue(topic, 0)
                        != expectedOffset
                        || controller.getMessageStore().dispatchBehindBytes() != 0) {
                        return false;
                    }
                }
                return true;
            });
    }

    private static void sendInitialSingleAndBatch(
        String namesrvAddr, String topic, String brokerName) throws Exception {
        DefaultMQProducer producer = startProducer(namesrvAddr);
        try {
            MessageQueue queue = awaitPublishQueue(producer, topic, brokerName);
            SendResult singleResult = producer.send(new Message(
                topic, "before-single".getBytes(StandardCharsets.UTF_8)), queue);
            assertSendResult(singleResult, brokerName, 0);
            Assert.assertNotNull(singleResult.getOffsetMsgId());

            List<Message> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                batch.add(new Message(topic,
                    ("before-batch-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            SendResult batchResult = producer.send(batch, queue);
            assertSendResult(batchResult, brokerName, 1);
            Assert.assertEquals(3, batchResult.getMsgId().split(",").length);
        } finally {
            producer.shutdown();
        }
    }

    private static void sendOne(String namesrvAddr, String topic, String brokerName,
        String body, long expectedQueueOffset) throws Exception {
        DefaultMQProducer producer = startProducer(namesrvAddr);
        try {
            MessageQueue queue = awaitPublishQueue(producer, topic, brokerName);
            SendResult result = producer.send(
                new Message(topic, body.getBytes(StandardCharsets.UTF_8)), queue);
            assertSendResult(result, brokerName, expectedQueueOffset);
            Assert.assertNotNull(result.getOffsetMsgId());
        } finally {
            producer.shutdown();
        }
    }

    private static DefaultMQProducer startProducer(String namesrvAddr)
        throws Exception {
        DefaultMQProducer producer =
            new DefaultMQProducer("dledger-it-" + UUID.randomUUID());
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        producer.setPollNameServerInterval(500);
        producer.setSendMsgTimeout(10_000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setVipChannelEnabled(false);
        producer.start();
        return producer;
    }

    private static MessageQueue awaitPublishQueue(DefaultMQProducer producer,
        String topic, String brokerName) {
        AtomicReference<MessageQueue> result = new AtomicReference<>();
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    MessageQueue queue = selectQueue(
                        producer.fetchPublishMessageQueues(topic), brokerName);
                    if (queue == null) {
                        return false;
                    }
                    result.set(queue);
                    return true;
                } catch (Exception ignored) {
                    return false;
                }
            });
        return result.get();
    }

    private static List<MessageExt> pullExactly(String namesrvAddr, String topic,
        String brokerName, int expectedCount) throws Exception {
        DefaultMQPullConsumer consumer =
            new DefaultMQPullConsumer("dledger-it-" + UUID.randomUUID());
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setPollNameServerInterval(500);
        consumer.setConsumerPullTimeoutMillis(3_000);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        AtomicReference<PullResult> resultRef = new AtomicReference<>();
        try {
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                    try {
                        MessageQueue queue = selectQueue(
                            consumer.fetchSubscribeMessageQueues(topic), brokerName);
                        if (queue == null) {
                            return false;
                        }
                        PullResult result = consumer.pull(
                            queue, "*", 0, Math.max(32, expectedCount));
                        if (result.getPullStatus() != PullStatus.FOUND
                            || result.getMinOffset() != 0
                            || result.getMaxOffset() != expectedCount
                            || result.getMsgFoundList().size() != expectedCount) {
                            return false;
                        }
                        resultRef.set(result);
                        return true;
                    } catch (Exception ignored) {
                        return false;
                    }
                });
            return new ArrayList<>(resultRef.get().getMsgFoundList());
        } finally {
            consumer.shutdown();
        }
    }

    private static MessageQueue selectQueue(
        Iterable<MessageQueue> queues, String brokerName) {
        for (MessageQueue queue : queues) {
            if (brokerName.equals(queue.getBrokerName()) && queue.getQueueId() == 0) {
                return queue;
            }
        }
        return null;
    }

    private static void assertSendResult(
        SendResult result, String brokerName, long expectedQueueOffset) {
        Assert.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        Assert.assertEquals(brokerName, result.getMessageQueue().getBrokerName());
        Assert.assertEquals(0, result.getMessageQueue().getQueueId());
        Assert.assertEquals(expectedQueueOffset, result.getQueueOffset());
        Assert.assertNotNull(result.getMsgId());
    }

    private static void assertBodies(
        List<MessageExt> messages, List<String> expectedBodies) {
        Assert.assertEquals(expectedBodies.size(), messages.size());
        for (int i = 0; i < expectedBodies.size(); i++) {
            MessageExt message = messages.get(i);
            Assert.assertEquals(i, message.getQueueOffset());
            Assert.assertArrayEquals(
                expectedBodies.get(i).getBytes(StandardCharsets.UTF_8), message.getBody());
        }
    }

    private static void stopControllers(List<BrokerController> controllers,
        Set<BrokerController> stopped) {
        for (BrokerController controller : new ArrayList<>(controllers)) {
            stopController(controller, stopped);
        }
    }

    private static void stopController(BrokerController controller,
        Set<BrokerController> stopped) {
        if (controller == null || !stopped.add(controller)) {
            return;
        }
        try {
            controller.shutdown();
        } catch (Throwable ignored) {
        }
    }

    private static List<Integer> allocatePorts(int count) throws IOException {
        List<ServerSocket> reservations = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        try {
            InetAddress loopback = InetAddress.getByName("127.0.0.1");
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0, 50, loopback);
                reservations.add(socket);
                ports.add(socket.getLocalPort());
            }
            return ports;
        } finally {
            closeSockets(reservations);
        }
    }

    private static String buildPeers(List<Integer> ports) {
        StringBuilder peers = new StringBuilder();
        for (int i = 0; i < NODE_IDS.size(); i++) {
            if (i > 0) {
                peers.append(';');
            }
            peers.append(NODE_IDS.get(i)).append("-127.0.0.1:").append(ports.get(i));
        }
        return peers.toString();
    }

    private static void awaitDLedgerPortsAvailable(List<NodeSpec> nodeSpecs) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> areDLedgerPortsAvailable(nodeSpecs));
    }

    private static boolean areDLedgerPortsAvailable(List<NodeSpec> nodeSpecs) {
        List<ServerSocket> probes = new ArrayList<>();
        try {
            InetAddress loopback = InetAddress.getByName("127.0.0.1");
            for (NodeSpec nodeSpec : nodeSpecs) {
                probes.add(new ServerSocket(nodeSpec.dLedgerPort, 50, loopback));
            }
            return true;
        } catch (IOException ignored) {
            return false;
        } finally {
            closeSockets(probes);
        }
    }

    private static void closeSockets(List<ServerSocket> sockets) {
        for (ServerSocket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class NodeSpec {
        private final String selfId;
        private final int dLedgerPort;
        private final String storeRoot;

        private NodeSpec(String selfId, int dLedgerPort, String storeRoot) {
            this.selfId = selfId;
            this.dLedgerPort = dLedgerPort;
            this.storeRoot = storeRoot;
        }
    }
}
```

The generated Bazel target is:

```text
//test:src/test/java/org/apache/rocketmq/test/dledger/DLedgerThreeNodeIT
```

Do not add it to `exclude_tests`. It must use three new producer/consumer instances across the initial, failover, and restart phases so stale client route caches cannot make the test pass.

- [ ] **Step 2: Run both the baseline single-node IT and the new three-node IT**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-dledger-broker-it.start
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-dledger-broker-it.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -pl test -am \
  -Pit-test -Pskip-unit-tests \
  -Dit.test=DLedgerProduceAndConsumeIT,DLedgerThreeNodeIT \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  verify
$ASSERT test/target/failsafe-reports/TEST-org.apache.rocketmq.test.dledger.DLedgerProduceAndConsumeIT.xml 1 "$MARKER"
$ASSERT test/target/failsafe-reports/TEST-org.apache.rocketmq.test.dledger.DLedgerThreeNodeIT.xml 1 "$MARKER"
```

Expected: both IT classes pass. In the new test, NameServer's master address must change to the promoted broker after leader shutdown and to the newly created broker after full restart; old messages are pulled before the first post-restart append.

- [ ] **Step 3: Commit the integration regression**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git diff --check
git add test/src/test/java/org/apache/rocketmq/test/dledger/DLedgerThreeNodeIT.java
git diff --cached --name-only
if git diff --cached --name-only | rg '^broker/null(?:/|$)'; then exit 1; fi
git commit -m "test: cover DLedger broker failover and restart"
```

## Task 9: Run the complete focused Maven validation matrix

**Files:**

- No production edits expected.
- Disposable-only create in both RocketMQ fixture worktrees:
  `store/src/test/java/org/apache/rocketmq/store/dledger/DLedgerUpgradeFixtureTest.java`
- Logs: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-*.log`

- [ ] **Step 1: Prove a coordinated full-stop upgrade can read RocketMQ/DLedger 0.3.2 data**

After Task 8's commits, create a clean latest-reader worktree at the exact adapted HEAD:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader
git worktree add --detach \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader \
  HEAD
test "$(git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader rev-parse HEAD)" \
  = "$(git rev-parse HEAD)"
test -z "$(git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader status --short)"
```

Using `apply_patch`, add the following complete validation-only class byte-for-byte to both the 0.3.2 writer worktree and the latest reader worktree. Do not add it to the shared branch:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.dledger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class DLedgerUpgradeFixtureTest extends MessageStoreTestBase {
    private static final String BASE = requiredProperty("upgrade.fixture.base");
    private static final String GROUP = requiredProperty("upgrade.fixture.group");
    private static final String PEERS = requiredProperty("upgrade.fixture.peers");
    private static final String TOPIC = requiredProperty("upgrade.fixture.topic");

    @Test
    public void writeWithRocketMQDLedger032() throws Exception {
        Assert.assertFalse(new File(BASE).exists());
        DefaultMessageStore store = null;
        boolean complete = false;
        try {
            store = createDledgerMessageStore(
                BASE, GROUP, "n0", PEERS, "n0", false, 0);
            putFixtureMessages(store, 0, 10);
            awaitStoreReady(store, 10);
            assertFixtureMessages(store, 10);
            complete = true;
        } finally {
            if (store != null && !store.isShutdown()) {
                store.shutdown();
            }
            if (complete) {
                baseDirs.remove(BASE);
            }
        }
    }

    @Test
    public void readWithLatestBeforeFirstUserWrite() throws Exception {
        Assert.assertTrue(new File(BASE).isDirectory());
        DefaultMessageStore store = null;
        try {
            store = createDledgerMessageStore(
                BASE, GROUP, "n0", PEERS, null, false, 0);
            final DefaultMessageStore reopened = store;
            await().atMost(20, SECONDS).pollInterval(100, MILLISECONDS).until(() ->
                ((DLedgerCommitLog) reopened.getCommitLog())
                    .getdLedgerServer().getMemberState().isLeader());
            awaitStoreReady(reopened, 10);

            // This is deliberately before the first append under the latest code.
            Assert.assertTrue(reopened.getMaxPhyOffset() > 0);
            Assert.assertEquals(0, reopened.getMinOffsetInQueue(TOPIC, 0));
            assertFixtureMessages(reopened, 10);

            putFixtureMessages(reopened, 10, 1);
            awaitStoreReady(reopened, 11);
            assertFixtureMessages(reopened, 11);
        } finally {
            if (store != null) {
                try {
                    if (!store.isShutdown()) {
                        store.shutdown();
                    }
                } finally {
                    store.destroy();
                }
            }
        }
    }

    private void awaitStoreReady(DefaultMessageStore store, long expectedMaxOffset) {
        await().atMost(20, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(expectedMaxOffset, store.getMaxOffsetInQueue(TOPIC, 0));
            Assert.assertEquals(0, store.dispatchBehindBytes());
        });
    }

    private void putFixtureMessages(DefaultMessageStore store, int firstIndex, int count)
        throws Exception {
        for (int index = firstIndex; index < firstIndex + count; index++) {
            MessageExtBrokerInner message = buildMessage();
            message.setTopic(TOPIC);
            message.setQueueId(0);
            message.setBody(body(index));
            message.putUserProperty("upgrade-index", Integer.toString(index));
            PutMessageResult result = store.putMessage(message);
            Assert.assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
            Assert.assertEquals(index, result.getAppendMessageResult().getLogicsOffset());
        }
    }

    private void assertFixtureMessages(DefaultMessageStore store, int count) {
        for (int index = 0; index < count; index++) {
            GetMessageResult result = store.getMessage(
                "upgrade-reader", TOPIC, 0, index, 1, null);
            Assert.assertNotNull(result);
            try {
                Assert.assertEquals(1, result.getMessageBufferList().size());
                MessageExt message = MessageDecoder.decode(result.getMessageBufferList().get(0));
                Assert.assertNotNull(message);
                Assert.assertEquals(TOPIC, message.getTopic());
                Assert.assertEquals(0, message.getQueueId());
                Assert.assertEquals(index, message.getQueueOffset());
                Assert.assertArrayEquals(body(index), message.getBody());
                Assert.assertEquals(
                    Integer.toString(index), message.getUserProperty("upgrade-index"));
            } finally {
                result.release();
            }
        }
    }

    private byte[] body(int index) {
        return ("upgrade-body-" + index).getBytes(StandardCharsets.UTF_8);
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing system property " + key);
        }
        return value;
    }
}
```

Before running, allocate one free loopback port and immutable fixture values, then save them in a shell file so both independent Maven invocations use exactly the same bytes:

```bash
set -euo pipefail
fixture_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/upgrade-from-0.3.2
test ! -e "$fixture_root"
fixture_port=$(/usr/bin/python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
mkdir -p "$fixture_root"
{
  printf 'FIXTURE_BASE=%q\n' "$fixture_root/store"
  printf 'FIXTURE_GROUP=%q\n' "upgrade-group-$(uuidgen)"
  printf 'FIXTURE_PEERS=%q\n' "n0-localhost:$fixture_port"
  printf 'FIXTURE_TOPIC=%q\n' "UpgradeTopic-$(uuidgen)"
} > "$fixture_root/fixture.env"
. "$fixture_root/fixture.env"
```

First prove the writer resolves the old graph and write/cleanly stop ten messages with RocketMQ's current DLedger 0.3.2 plus fastjson1:

```bash
set -euo pipefail
fixture_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/upgrade-from-0.3.2
. "$fixture_root/fixture.env"
WRITER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-0.3.2-writer
RUN=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged
JDK8=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8
M2=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2
TMP=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-writer.start

cd "$WRITER"
$RUN /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-writer-dependencies.log \
  $JDK8 mvn -B -ntp -nsu -Dmaven.repo.local="$M2" -pl store -am dependency:tree \
  -Dverbose -Dincludes=com.alibaba:fastjson,io.openmessaging.storage:dledger
rg -F 'io.openmessaging.storage:dledger:jar:0.3.2' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-writer-dependencies.log
rg -F 'com.alibaba:fastjson:jar:1.2.83' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-writer-dependencies.log

touch "$MARKER"
$RUN /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-writer.log \
  $JDK8 mvn -B -ntp -nsu -Dmaven.repo.local="$M2" -Djava.io.tmpdir="$TMP" \
  -Dupgrade.fixture.base="$FIXTURE_BASE" -Dupgrade.fixture.group="$FIXTURE_GROUP" \
  -Dupgrade.fixture.peers="$FIXTURE_PEERS" -Dupgrade.fixture.topic="$FIXTURE_TOPIC" \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerUpgradeFixtureTest#writeWithRocketMQDLedger032' test
$ASSERT "$WRITER/store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerUpgradeFixtureTest.xml" 1 "$MARKER"
test -d "$FIXTURE_BASE/dledger-n0/data"
if ps -axo pid=,command= | awk -v root="$WRITER" \
  'index($0, root) && /[j]ava/ {found=1} END {exit found ? 0 : 1}'; then
  exit 1
fi
fixture_port=${FIXTURE_PEERS##*:}
if lsof -nP -iTCP:"$fixture_port" -sTCP:LISTEN; then exit 1; fi
```

The explicit process and listener checks prove the writer JVM and DLedger port have exited. Then run the latest reader against the exact same directory and require old messages before the first latest-code append:

```bash
set -euo pipefail
fixture_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/upgrade-from-0.3.2
. "$fixture_root/fixture.env"
RUN=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged
JDK8=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8
M2=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2
TMP=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp
READER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-latest-reader
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
MARKER=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-latest-reader.start
cd "$READER"
touch "$MARKER"
$RUN /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-upgrade-latest-reader.log \
  $JDK8 mvn -B -ntp -nsu -Dmaven.repo.local="$M2" -Djava.io.tmpdir="$TMP" \
  -Dupgrade.fixture.base="$FIXTURE_BASE" -Dupgrade.fixture.group="$FIXTURE_GROUP" \
  -Dupgrade.fixture.peers="$FIXTURE_PEERS" -Dupgrade.fixture.topic="$FIXTURE_TOPIC" \
  -pl store -am -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dtest=DLedgerUpgradeFixtureTest#readWithLatestBeforeFirstUserWrite' test
$ASSERT "$READER/store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerUpgradeFixtureTest.xml" 1 "$MARKER"
test ! -e "$FIXTURE_BASE"
if ps -axo pid=,command= | awk -v root="$READER" \
  'index($0, root) && /[j]ava/ {found=1} END {exit found ? 0 : 1}'; then
  exit 1
fi
fixture_port=${FIXTURE_PEERS##*:}
if lsof -nP -iTCP:"$fixture_port" -sTCP:LISTEN; then exit 1; fi
```

Expected: the writer log proves DLedger 0.3.2 plus fastjson1 1.2.83; the latest reader uses the adapted HEAD, recovers and reads all ten old messages before its first append, then writes/reads message eleven. This validates a coordinated full-stop data upgrade, not mixed-version networking or downgrade.

- [ ] **Step 2: Verify dependencies before long tests**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-dependencies.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -Dfastjson2.version=2.0.64 \
  -pl common,remoting,store,controller,broker,container \
  dependency:tree \
  -Dverbose \
  -Dincludes=com.alibaba:fastjson,com.alibaba.fastjson2:fastjson2,io.openmessaging.storage:dledger,org.apache.rocketmq:rocketmq-remoting
```

Expected: one DLedger PR #336 coordinate, one fastjson2 2.0.64, reactor remoting, and zero resolved fastjson1.

- [ ] **Step 3: Package all affected modules**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-package.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -pl broker,controller,store,container,test -am \
  -DskipTests package
```

- [ ] **Step 4: Run focused unit and lifecycle tests**

Run separately so every failure has one owner:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
ASSERT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/assert-junit-report
LOGS=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs
MARKER="$LOGS/rocketmq-maven-remoting.start"
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-remoting.log /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 mvn -B -ntp -nsu -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp -pl remoting -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=RemotingSerializableCompatTest test
$ASSERT remoting/target/surefire-reports/TEST-org.apache.rocketmq.remoting.protocol.RemotingSerializableCompatTest.xml 5 "$MARKER"
MARKER="$LOGS/rocketmq-maven-store.start"
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-store.log /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 mvn -B -ntp -nsu -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp -pl store -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=DLedgerLatestCommitLogTest test
$ASSERT store/target/surefire-reports/TEST-org.apache.rocketmq.store.dledger.DLedgerLatestCommitLogTest.xml 8 "$MARKER"
MARKER="$LOGS/rocketmq-maven-controller.start"
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-controller.log /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 mvn -B -ntp -nsu -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp -pl controller -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=DLedgerControllerTest,ControllerManagerTest test
$ASSERT controller/target/surefire-reports/TEST-org.apache.rocketmq.controller.impl.DLedgerControllerTest.xml 6 "$MARKER"
$ASSERT controller/target/surefire-reports/TEST-org.apache.rocketmq.controller.ControllerManagerTest.xml 1 "$MARKER"
MARKER="$LOGS/rocketmq-maven-container.start"
touch "$MARKER"
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-container.log /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 mvn -B -ntp -nsu -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp -pl container -am -Dsurefire.failIfNoSpecifiedTests=false '-Dtest=BrokerContainerTest#testAddAndRemoveDLedgerBroker' test
$ASSERT container/target/surefire-reports/TEST-org.apache.rocketmq.container.BrokerContainerTest.xml 1 "$MARKER"
```

Expected counts: remoting 5, new store class all methods, controller 7 total (six `DLedgerControllerTest` methods plus `ControllerManagerTest`), container lifecycle 1.

- [ ] **Step 5: Run a fresh full JDK 8 unit reactor**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-full-unit.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -Dfastjson2.version=2.0.64 \
  clean test
```

Classify unrelated, reproducible baseline flakes separately; do not call the scheme green while any DLedger, fastjson, store, controller, broker, or dependency error remains.

## Task 10: Validate Bazel in a disposable RocketMQ copy

**Files:**

- Create disposable clone: `/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel`
- Disposable-only modify: `WORKSPACE`
- Disposable-only modify: `bazel/GenTestRules.bzl`
- Do not modify the shared RocketMQ worktree.

- [ ] **Step 1: Freeze the shared diff and create a disposable copy**

Record:

```bash
set -euo pipefail
set -o pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git status --short
git diff 293f5885719fc4aa3619446a1900f58ccfcfdd29...HEAD -- . ':!broker/null' \
  | shasum -a 256 \
  | tee /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/shared-diff.sha256

test ! -e /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel
git worktree add --detach \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel \
  HEAD
test "$(git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel rev-parse HEAD)" \
  = "$(git rev-parse HEAD)"
```

Because the disposable worktree is created directly at the branch HEAD, every committed product file is byte-identical before local-only verification changes. Confirm `git -C .../rocketmq-bazel status --short` is empty.

- [ ] **Step 2: Serve the isolated Maven repository and add it first in disposable WORKSPACE**

Prepare sidecars and verify port 18086:

```bash
set -euo pipefail
if lsof -nP -iTCP:18086 -sTCP:LISTEN; then exit 1; fi
dledger_repo=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2/io/openmessaging/storage
while IFS= read -r artifact_file; do
  shasum "$artifact_file" | awk '{print $1}' > "$artifact_file.sha1"
done < <(find "$dledger_repo" -type f \( -name '*.jar' -o -name '*.pom' \) -print)

expected_dledger_sha=$(awk '{print $1}' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-jar.sha256)
test -n "$expected_dledger_sha"
```

Start this exact command in a dedicated long-running exec session and retain its session ID for Step 7:

```bash
set -euo pipefail
/usr/bin/python3 -m http.server 18086 \
  --bind 127.0.0.1 \
  --directory /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2
```

From another shell, require the same JAR bytes over HTTP:

```bash
set -euo pipefail
expected_dledger_sha=$(awk '{print $1}' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/dledger-pr336-jar.sha256)
test -n "$expected_dledger_sha"
actual_dledger_sha=$(curl -fsS \
  http://127.0.0.1:18086/io/openmessaging/storage/dledger/0.3.3-pr336-f2-64-SNAPSHOT/dledger-0.3.3-pr336-f2-64-SNAPSHOT.jar \
  | shasum -a 256 | awk '{print $1}')
test "$actual_dledger_sha" = "$expected_dledger_sha"
```

Using `apply_patch` in the disposable worktree only, add this repository first in WORKSPACE:

```starlark
"http://127.0.0.1:18086",
```

Generate `.sha1` sidecars for the local DLedger POM and JAR if absent, and verify the HTTP GET returns the same SHA-256 as Task 2.

- [ ] **Step 3: Force Bazel test JVMs onto the low-usage volume**

In the disposable `bazel/GenTestRules.bzl`, append the final JVM flag after `-Dbuild.bazel=true`:

```starlark
jvm_flags = jvm_flags + [
    "-Dbuild.bazel=true",
    "-Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp",
],
```

Also replace the disposable WORKSPACE `@jdk8` `java_home` with:

```starlark
java_home = "/private/tmp/rocketmq-dledger-fastjson2/tools/amazon-corretto-8.jdk/Contents/Home",
```

These machine-local changes must never appear in the shared diff. Run `git -C .../rocketmq-bazel diff --check` before Bazel.

- [ ] **Step 4: Build the complete affected Bazel graph**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-build.log \
  /private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0 \
  --output_user_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output \
  build \
  --repository_cache=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-repository-cache \
  --java_runtime_version=8 \
  --action_env=JAVA_HOME=/private/tmp/rocketmq-dledger-fastjson2/tools/amazon-corretto-8.jdk/Contents/Home \
  //store:store //controller:controller //broker:broker //container:container //test:tests
```

- [ ] **Step 5: Confirm Bazel resolved the intended graph**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-fastjson2-target.log \
  /private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0 \
  --output_user_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output \
  query --output=build '@maven//:com_alibaba_fastjson2_fastjson2'

/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-target.log \
  /private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0 \
  --output_user_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output \
  query --output=build '@maven//:io_openmessaging_storage_dledger'

rg -F 'maven_coordinates=com.alibaba.fastjson2:fastjson2:2.0.64' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-fastjson2-target.log
rg -F 'maven_coordinates=io.openmessaging.storage:dledger:0.3.3-pr336-f2-64-SNAPSHOT' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-target.log

set -o pipefail
BAZEL=/private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0
ROOT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output
output_base=$($BAZEL --output_user_root="$ROOT" info output_base)
generated_maven_build="$output_base/external/maven/BUILD"
{
  test -s "$generated_maven_build"
  if rg 'name = "com_alibaba_fastjson"' "$generated_maven_build"; then
    exit 1
  else
    rc=$?
    test "$rc" -eq 1
  fi
  if rg 'maven_coordinates=org.apache.rocketmq:rocketmq-remoting:' "$generated_maven_build"; then
    exit 1
  else
    rc=$?
    test "$rc" -eq 1
  fi
  echo 'ABSENT @maven//:com_alibaba_fastjson'
  echo 'ABSENT external org.apache.rocketmq:rocketmq-remoting'
} 2>&1 | tee /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-absence.log
```

Expected: the generated targets' `maven_coordinates` tags prove the exact versions; fastjson1 and external `rocketmq-remoting` targets do not exist.

- [ ] **Step 6: Run the Bazel compatibility, store, controller-manager, and real broker tests**

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel
BAZEL=/private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0
ROOT=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output
CACHE=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-repository-cache
RUN_LOGGED=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged

$RUN_LOGGED /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-remoting.log $BAZEL --output_user_root="$ROOT" test --repository_cache="$CACHE" \
  --java_runtime_version=8 --nocache_test_results --test_output=all --local_test_jobs=1 \
  '//remoting:src/test/java/org/apache/rocketmq/remoting/protocol/RemotingSerializableCompatTest'
rg -F 'OK (5 tests)' /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-remoting.log

$RUN_LOGGED /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-store.log $BAZEL --output_user_root="$ROOT" test --repository_cache="$CACHE" \
  --java_runtime_version=8 --nocache_test_results --test_output=all --local_test_jobs=1 \
  '//store:src/test/java/org/apache/rocketmq/store/dledger/DLedgerLatestCommitLogTest'
rg -F 'OK (8 tests)' /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-store.log

$RUN_LOGGED /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-controller.log $BAZEL --output_user_root="$ROOT" test --repository_cache="$CACHE" \
  --java_runtime_version=8 --nocache_test_results --test_output=all --local_test_jobs=1 \
  '//controller:src/test/java/org/apache/rocketmq/controller/ControllerManagerTest'
rg -F 'OK (1 test)' /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-controller.log

$RUN_LOGGED /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-it.log $BAZEL --output_user_root="$ROOT" test --repository_cache="$CACHE" \
  --java_runtime_version=8 --nocache_test_results --test_output=all --local_test_jobs=1 \
  '//test:src/test/java/org/apache/rocketmq/test/dledger/DLedgerProduceAndConsumeIT'
rg -F 'OK (1 test)' /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-it.log

$RUN_LOGGED /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-three-node-it.log $BAZEL --output_user_root="$ROOT" test --repository_cache="$CACHE" \
  --java_runtime_version=8 --nocache_test_results --test_output=all --local_test_jobs=1 \
  '//test:src/test/java/org/apache/rocketmq/test/dledger/DLedgerThreeNodeIT'
rg -F 'OK (1 test)' /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-three-node-it.log
```

For each DLedger test, prove from the real Java process or Netty log that the last `-Djava.io.tmpdir` points to the validation volume and that `diskFull=true` is absent. A host `/tmp` disk-full failure is not a product failure and must not be papered over.

- [ ] **Step 7: Cleanly stop Bazel and the HTTP server, then prove shared-tree integrity**

Run:

```bash
set -euo pipefail
cd /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel
/private/tmp/rocketmq-dledger-fastjson2/tools/bazel-6.5.0 \
  --output_user_root=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/bazel-output \
  shutdown
```

Send Ctrl-C only to the retained HTTP exec session and wait for it to exit. Verify `lsof -nP -iTCP:18086 -sTCP:LISTEN` and a process search for this Bazel output root/test JVMs return no owners. Restore only the two disposable edits with:

```bash
set -euo pipefail
set -o pipefail
git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel \
  diff -- WORKSPACE bazel/GenTestRules.bzl \
  | git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel apply -R
test -z "$(git -C /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/rocketmq-bazel status --short)"
```

Finally, prove the retained server, test JVMs, and shared diff all returned to the frozen state:

```bash
set -euo pipefail
if lsof -nP -iTCP:18086 -sTCP:LISTEN; then exit 1; fi
if ps -axo pid=,command= \
  | rg '[b]azel-output|[h]ttp\.server 18086|[D]LedgerThreeNodeIT|[D]LedgerProduceAndConsumeIT'; then
  exit 1
fi
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
expected_shared_sha=$(awk '{print $1}' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/shared-diff.sha256)
actual_shared_sha=$(git diff 293f5885719fc4aa3619446a1900f58ccfcfdd29...HEAD \
  -- . ':!broker/null' | shasum -a 256 | awk '{print $1}')
test -n "$expected_shared_sha"
test "$actual_shared_sha" = "$expected_shared_sha"
```

## Task 11: Prove fastjson1 is absent from source, resolution, and packaged bytecode

**Files:**

- Inspect all changed source/build files.
- Inspect JARs under `common`, `remoting`, `store`, `controller`, `broker`, `container`, and the installed DLedger coordinate.

- [ ] **Step 1: Run a zero-source/build-reference scan in RocketMQ**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
if rg -n --pcre2 \
  'com\.alibaba:fastjson(?!2)|com\.alibaba\.fastjson(?!2)|com_alibaba_fastjson(?!2)|<fastjson\.version>|<artifactId>fastjson</artifactId>' \
  pom.xml \
  common remoting store controller container \
  broker/pom.xml broker/BUILD.bazel broker/src \
  WORKSPACE \
  --glob '!**/target/**'; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi
```

Expected: no matches. The DLedger POM exclusion is outside this RocketMQ scan and is allowed only as guard metadata.

- [ ] **Step 2: Repackage after the clean reactor, then scan JAR entries and every class constant pool**

Task 9's final `clean test` removes the JARs produced by its earlier package step, so first rebuild the affected binary artifacts without rerunning tests:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-logged \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-package-for-bytecode.log \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/run-jdk8 \
  mvn -B -ntp -nsu \
  -Dmaven.repo.local=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2 \
  -Djava.io.tmpdir=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/tmp \
  -Ddleger.version=0.3.3-pr336-f2-64-SNAPSHOT \
  -Dfastjson2.version=2.0.64 \
  -pl common,remoting,store,controller,broker,container -am \
  -DskipTests package
```

Then run this complete audit; it retains its uniquely named extraction directory for inspection:

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
export JAVA_HOME=/private/tmp/rocketmq-dledger-fastjson2/tools/amazon-corretto-8.jdk/Contents/Home
audit_root=$(mktemp -d /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/jar-audit.XXXXXX)
audit_log=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-fastjson1-bytecode-audit.log
audit_failed=0
audit_count=0
dledger_jar=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/m2/io/openmessaging/storage/dledger/0.3.3-pr336-f2-64-SNAPSHOT/dledger-0.3.3-pr336-f2-64-SNAPSHOT.jar
test -s "$dledger_jar"

for module_dir in common remoting store controller broker container; do
  module_jar_count=$(find "$module_dir/target" -maxdepth 1 -type f -name '*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' | wc -l | tr -d ' ')
  if test "$module_jar_count" -lt 1; then
    echo "ERROR no packaged binary JAR for $module_dir"
    exit 1
  fi
done

jar_list=$(mktemp "$audit_root/jars.XXXXXX")
find common/target remoting/target store/target controller/target broker/target container/target \
  -maxdepth 1 -type f -name '*.jar' \
  ! -name '*-sources.jar' \
  ! -name '*-javadoc.jar' \
  -print0 > "$jar_list"
printf '%s\0' "$dledger_jar" >> "$jar_list"

{
  while IFS= read -r -d '' jar_file; do
    echo "AUDIT $jar_file"
    audit_count=$((audit_count + 1))
    entries_file=$(mktemp "$audit_root/entries.XXXXXX")
    if ! "$JAVA_HOME/bin/jar" tf "$jar_file" > "$entries_file"; then
      echo "ERROR cannot list JAR: $jar_file"
      audit_failed=1
      continue
    fi
    if rg '^com/alibaba/fastjson/' "$entries_file"; then
      echo "ERROR embedded fastjson1 classes: $jar_file"
      audit_failed=1
    else
      rc=$?
      if test "$rc" -ne 1; then
        echo "ERROR cannot scan JAR entries: $jar_file (rg=$rc)"
        audit_failed=1
      fi
    fi

    jar_dir=$(mktemp -d "$audit_root/jar.XXXXXX")
    if ! (
      cd "$jar_dir"
      "$JAVA_HOME/bin/jar" xf "$jar_file"
    ); then
      echo "ERROR cannot extract JAR: $jar_file"
      audit_failed=1
      continue
    fi
    class_list=$(mktemp "$audit_root/classes.XXXXXX")
    find "$jar_dir" -type f -name '*.class' -print0 > "$class_list"
    while IFS= read -r -d '' class_file; do
      strings_file=$(mktemp "$audit_root/strings.XXXXXX")
      if ! strings "$class_file" > "$strings_file"; then
        echo "ERROR cannot inspect class: $jar_file :: $class_file"
        audit_failed=1
        continue
      fi
      if rg -q --pcre2 'com[/\.]alibaba[/\.]fastjson(?!2)' "$strings_file"; then
        echo "ERROR fastjson1 constant-pool reference: $jar_file :: $class_file"
        rg --pcre2 'com[/\.]alibaba[/\.]fastjson(?!2)' "$strings_file"
        audit_failed=1
      else
        rc=$?
        if test "$rc" -ne 1; then
          echo "ERROR cannot scan class strings: $jar_file :: $class_file (rg=$rc)"
          audit_failed=1
        fi
      fi
    done < "$class_list"
  done < "$jar_list"
  test "$audit_count" -ge 7
  test "$audit_failed" -eq 0
} 2>&1 | tee "$audit_log"
```

Expected: every JAR is listed, with no `ERROR` line and final exit code 0.

- [ ] **Step 3: Reconfirm effective versions from both build systems**

Run:

```bash
set -euo pipefail
maven_log=/private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-maven-dependencies.log
rg -F 'com.alibaba.fastjson2:fastjson2:jar:2.0.64' "$maven_log"
rg -F 'io.openmessaging.storage:dledger:jar:0.3.3-pr336-f2-64-SNAPSHOT' "$maven_log"
if rg -F 'com.alibaba:fastjson:jar:' "$maven_log"; then
  exit 1
else
  rc=$?
  test "$rc" -eq 1
fi

rg -F 'maven_coordinates=com.alibaba.fastjson2:fastjson2:2.0.64' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-fastjson2-target.log
rg -F 'maven_coordinates=io.openmessaging.storage:dledger:0.3.3-pr336-f2-64-SNAPSHOT' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-dledger-target.log

rg -F 'ABSENT @maven//:com_alibaba_fastjson' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-absence.log
rg -F 'ABSENT external org.apache.rocketmq:rocketmq-remoting' \
  /private/tmp/rocketmq-dledger-fastjson2/pr336-latest-validation/logs/rocketmq-bazel-absence.log
```

These assertions are the exact Maven/Bazel version and exclusion evidence; a label-only query is insufficient.

## Task 12: Final review, commit integrity, and local handoff

**Files:**

- Review every commit after `85872858ec38a3a1f99a7e2d61f7f39406136818`.
- Do not create or push a community PR.

- [ ] **Step 1: Run final repository checks**

```bash
set -euo pipefail
cd '/Users/jinrongtong/.codex/worktrees/9e8c/RocketMQ 开源'
git diff --check 293f5885719fc4aa3619446a1900f58ccfcfdd29...HEAD
git status --short --branch
git log --oneline --decorate 293f5885719fc4aa3619446a1900f58ccfcfdd29..HEAD
git diff --stat 293f5885719fc4aa3619446a1900f58ccfcfdd29...HEAD
```

Expected status: clean tracked tree plus only the pre-existing `?? broker/null/`.

- [ ] **Step 2: Perform an independent code review**

Review for these specific failure modes:

- Any read path exposes bytes beyond `committedPos`.
- `truncate` can compute a negative size.
- cache publication order is reversed.
- committed index falls back to ledger end.
- restart test writes a user entry before checking old data.
- NOOP changes CQ offsets or user-message count.
- batch future base position is not the final element in `getPositions()`.
- state-machine iterator unwraps or applies an entry more than once.
- Maven and Bazel versions drift.
- fastjson1 remains only because a test target still depends on it.
- disposable localhost/JDK/tmpdir settings leaked into the product diff.

Resolve every Critical or Important finding and rerun the affected RED/GREEN and integration gates before completion.

- [ ] **Step 3: Produce the evidence report**

Report:

- Exact RocketMQ/DLedger commits and DLedger artifact SHA-256.
- DLedger master RED and PR #336 GREEN results.
- fastjson2 2.0.63 JDK 8 RED and 2.0.64 GREEN.
- Maven/Bazel build and test counts.
- Three-node leader/failover and two-restart results.
- Dependency-tree and bytecode zero-fastjson1 results.
- The coordinated full-stop-only compatibility boundary.
- The pre-existing unsupported combination `enableDLegerCommitLog=true` plus
  `enableBuildConsumeQueueConcurrently=true`; the default sequential reput path is the validated path.
- Any unrelated baseline flake, with separate evidence and no false green claim.

Do not push the branch. The next publishable step is to wait for a public DLedger version containing PR #336, replace the local coordinate, and rerun this complete matrix from a clean checkout.
