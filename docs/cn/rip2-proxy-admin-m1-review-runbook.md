# RIP-2 Proxy Admin M1 评审复现手册

本文帮助评审从两个 draft 分支复现生成版公开 `ProxyAdminService` endpoint。
命令假设当前工作目录是 `rocketmq` 仓库根目录，配套 API 仓库位于
`../rocketmq-apis`。

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
git checkout rip2-proxy-admin-m1
git pull --ff-only origin rip2-proxy-admin-m1
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
Tests run: 38, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

JDK 17 下 JaCoCo 0.8.5 可能会对 JDK 和 Mockito 生成类打印 instrumentation
stack traces。只有在 Surefire 零 failure/error 且 Maven 成功退出时，才把这些
日志视为环境噪声。

## 5. 运行 broad proxy admin 验证

```bash
mvn -pl proxy -am \
  "-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
  -DfailIfNoTests=false test -DskipITs
```

最终记录的预期结果：

```text
Tests run: 709, Failures: 0, Errors: 0, Skipped: 0
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

```bash
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-acceptance-audit.md
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-submission-package.md
sed -n '1,120p' docs/cn/rip2-proxy-admin-m1-final-smoke.md
```

验收审计会区分本地已验证项和外部门禁。剩余门禁为：

- `rocketmq-apis` public proto proposal 被社区接受。
- 发布包含 `ProxyAdminServiceGrpc` 的正式 `rocketmq-proto` artifact。
- 在包含 RIP-1 dashboard client 的环境中完成 Dashboard CLIENT-01 联调。
