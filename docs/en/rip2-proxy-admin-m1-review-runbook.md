# RIP-2 Proxy Admin M1 Review Runbook

This runbook lets a reviewer reproduce the generated public
`ProxyAdminService` endpoint from the two draft branches. Commands assume the
current working directory is the `rocketmq` repository root and the companion
API checkout is available at `../rocketmq-apis`.

Related public API discussion docs:

- `docs/en/rip2-proxy-admin-public-api-discussion.md`
- `docs/cn/rip2-proxy-admin-public-api-discussion.md`

## 1. Prerequisites

Use JDK 17, Maven, Bazel, Git, and `rg`. Ensure JDK 17 is the active Java on
`PATH` before running Maven.

```bash
java -version
mvn -version
bazel --version
rg --version
```

Expected: Java reports version 17, Maven and Bazel are available, and `rg`
prints its version.

## 2. Prepare The API Branch

If `../rocketmq-apis` does not exist yet, clone the fork beside this repository:

```bash
cd ..
git clone https://github.com/pilichoumao/rocketmq-apis.git rocketmq-apis
cd rocketmq
```

Build and install the local proposal artifact:

```bash
cd ../rocketmq-apis
git fetch origin
git checkout rip2-proxy-admin-public-api
git pull --ff-only origin rip2-proxy-admin-public-api

bazel build //java:rocketmq-proto
PROTO_JAR="$(find bazel-bin -type f -name '*rocketmq-proto*.jar' | sort | head -n 1)"
test -n "$PROTO_JAR"
jar tf "$PROTO_JAR" | rg 'apache/rocketmq/v2/(ProxyAdminServiceGrpc|ListClientsRequest|ProxyClient)\.class'

mvn install:install-file \
  -Dfile="$PROTO_JAR" \
  -DgroupId=org.apache.rocketmq \
  -DartifactId=rocketmq-proto \
  -Dversion=2.2.0-rip2-SNAPSHOT \
  -Dpackaging=jar
```

Expected:

```text
BUILD SUCCESS
apache/rocketmq/v2/ProxyAdminServiceGrpc.class
apache/rocketmq/v2/ListClientsRequest.class
apache/rocketmq/v2/ProxyClient.class
```

## 3. Prepare The RocketMQ Branch

```bash
cd ../rocketmq
git fetch origin
git checkout rip2-proxy-admin-m1
git pull --ff-only origin rip2-proxy-admin-m1
git status --short --branch --untracked-files=all
rg -n '<rocketmq-proto.version>2.2.0-rip2-SNAPSHOT</rocketmq-proto.version>' pom.xml
```

Expected:

```text
## rip2-proxy-admin-m1...origin/rip2-proxy-admin-m1
```

## 4. Run Focused Public Endpoint Verification

```bash
mvn -pl proxy -am \
  -Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest \
  -DfailIfNoTests=false test -DskipITs
```

Expected:

```text
Tests run: 51, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

JDK 17 may print JaCoCo 0.8.5 instrumentation stack traces for JDK and
Mockito-generated classes. Treat them as environment noise only when Surefire
reports zero failures/errors and Maven exits successfully.

## 5. Run Broad Proxy Admin Verification

```bash
mvn -pl proxy -am \
  "-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
  -DfailIfNoTests=false test -DskipITs
```

Expected from the recorded final run:

```text
Tests run: 724, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## 6. Run Package Smoke

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

Expected:

```text
BUILD SUCCESS
```

## 7. Inspect Submission Evidence

```bash
sed -n '1,120p' docs/en/rip2-proxy-admin-m1-acceptance-audit.md
sed -n '1,120p' docs/en/rip2-proxy-admin-m1-submission-package.md
sed -n '1,120p' docs/en/rip2-proxy-admin-m1-final-smoke.md
```

The acceptance audit distinguishes locally verified items from external gates.
The remaining gates are:

- `rocketmq-apis` public proto proposal acceptance.
- official `rocketmq-proto` artifact publication with `ProxyAdminServiceGrpc`.
- Dashboard CLIENT-01 joint E2E in an environment that contains the RIP-1
  dashboard client.
