# RIP-2 Proxy Admin M1 评审复现手册

本文帮助评审从两个 draft 分支复现生成版公开 `ProxyAdminService` endpoint。
命令假设当前工作目录是 `rocketmq` 仓库根目录，配套 API 仓库位于
`../rocketmq-apis`。

相关 public API 讨论文档：

- `docs/en/rip2-proxy-admin-public-api-discussion.md`
- `docs/cn/rip2-proxy-admin-public-api-discussion.md`

## 1. 前置依赖

使用 JDK 17、Maven、Bazel、Git 和 `rg`。运行 Maven 前请确保 JDK 17 是
`PATH` 上的当前 Java。

```bash
java -version
mvn -version
bazel --version
rg --version
```

预期：Java 为 17，Maven、Bazel 和 `rg` 均可用。

## 2. 准备 API 分支

如果 `../rocketmq-apis` 还不存在，先在当前仓库同级目录 clone fork：

```bash
cd ..
git clone https://github.com/pilichoumao/rocketmq-apis.git rocketmq-apis
cd rocketmq
```

构建并安装本地 proposal artifact：

```bash
cd ../rocketmq-apis
git fetch origin
git checkout -B rip2-proxy-admin-public-api c372905ce927cf8957333e7ac07877f295fd7ec9
test "$(git rev-parse HEAD)" = c372905ce927cf8957333e7ac07877f295fd7ec9

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

预期：

```text
BUILD SUCCESS
apache/rocketmq/v2/ProxyAdminServiceGrpc.class
apache/rocketmq/v2/ListClientsRequest.class
apache/rocketmq/v2/ProxyClient.class
```

## 3. 准备 RocketMQ 分支

```bash
cd ../rocketmq
git fetch origin
git checkout -B rip2-proxy-admin-m1 1dd5c6fd1f7e8f5d684213d72c4b965d214f977d
test "$(git rev-parse HEAD)" = 1dd5c6fd1f7e8f5d684213d72c4b965d214f977d
git status --short --branch --untracked-files=all
rg -n '<rocketmq-proto.version>2.2.0-rip2-SNAPSHOT</rocketmq-proto.version>' pom.xml
```

预期：

```text
## rip2-proxy-admin-m1...origin/rip2-proxy-admin-m1
```

## 4. 运行 public endpoint 聚焦验证

```bash
mvn -pl proxy -am \
  -Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest \
  -DfailIfNoTests=false test -DskipITs
```

预期：

```text
Tests run: 56, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

JDK 17 下 JaCoCo 0.8.5 可能会对 JDK 和 Mockito 生成类打印 instrumentation
stack traces。只有在 Surefire 零 failure/error 且 Maven 成功退出时，才把这些
日志视为环境噪声。

## 5. 运行 broad proxy admin 验证

```bash
mvn -pl proxy -am \
  "-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
  -DfailIfNoTests=false test -DskipITs
```

最终记录的预期结果：

```text
Tests run: 767, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## 6. 运行 package smoke

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

预期：

```text
BUILD SUCCESS
```

## 7. 查看提交证据

先运行轻量提交门禁：

```bash
python3 dev/rip2_submission_guard.py
```

预期：

```text
RIP-2 submission guard passed.
```

如果正在验证 fork 分支本身，可以追加 `--check-remote`，确认
`origin/rip2-proxy-admin-m1` 与 `HEAD` 一致；追加 `--check-apis-remote`，确认
`../rocketmq-apis` 的 `rip2-proxy-admin-public-api` 分支与它配置的 upstream
remote 一致。API remote 默认使用 `auto`，因此 reviewer 从 fork clone、remote 名为
`origin` 的场景，以及本机 checkout 使用 `fork` remote 的场景都能使用同一个命令。
追加 `--check-github` 还会确认两个 draft PR 描述和 RIP-2 issue comment 均引用同一个
RocketMQ HEAD 和 rocketmq-apis HEAD，并包含提交门禁证据：

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github
```

最终 release gate 还需要强制两个 public PR 都上报 checks，并拒绝任何非
passing 或非 explicitly skipped 的 check：

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --require-github-checks
```

当前 draft PR 没有 reported checks，因此在 Apache GitHub Actions 实际运行前，
严格命令预期保持红色。上面的非严格命令是当前可复现的 draft-review gate。

```bash
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-acceptance-audit.md
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-submission-package.md
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-final-smoke.md
sed -n '1,160p' docs/cn/rip2-proxy-admin-m1-dashboard-contract.md
```

验收审计会区分本地已验证项和外部门禁。剩余门禁为：

- `rocketmq-apis` public proto proposal 被社区接受。
- 发布包含 `ProxyAdminServiceGrpc` 的正式 `rocketmq-proto` artifact。
- 在包含 RIP-1 dashboard client 的环境中完成 Dashboard CLIENT-01 联调。字段级
  契约已记录在 `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。
