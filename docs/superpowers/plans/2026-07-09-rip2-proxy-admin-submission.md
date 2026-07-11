# RIP-2 Proxy Admin Submission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the current `rip2-proxy-admin-m1` branch so it can be submitted for the Taiyi RocketMQ RIP-2 contest as a real public Proxy Admin gRPC M1 implementation, not only an endpoint-ready foundation.

**Architecture:** Keep the existing read model, lifecycle writes, ACL, metrics, and endpoint-ready adapter. Close the remaining gap by landing the formal `ProxyAdminService` protobuf in `rocketmq-apis`, upgrading this repo to the generated `rocketmq-proto` artifact, adding a thin generated-proto `GrpcProxyAdminApplication`, wiring it into the admin gRPC server, and refreshing submission evidence.

**Tech Stack:** Apache RocketMQ proxy module, Java 17, Maven, gRPC Java, protobuf generated classes from `org.apache.rocketmq:rocketmq-proto`, Bazel for `apache/rocketmq-apis`.

---

## Current State Summary

This plan assumes the worker starts in:

```bash
cd .
git status --short --branch --untracked-files=all
```

All paths in this plan are relative to the current `rocketmq` repository root.
The companion `rocketmq-apis` checkout is expected to live beside this repo at
`../rocketmq-apis`.

Expected branch:

```text
## rip2-proxy-admin-m1...origin/rip2-proxy-admin-m1
```

Known completed work:

- `proxy/src/main/java/org/apache/rocketmq/proxy/service/admin/client/ProxyClientReadService.java` already stores and indexes online gRPC client state.
- `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/client/ClientActivity.java` already writes lifecycle state.
- `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/ProxyClientAdminEndpointExecutor.java` already exposes endpoint-ready methods for `listClients`, `describeClient`, `listClientsByGroup`, and `listClientsByTopic`.
- `proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java` already has an admin gRPC server seam, but `createProxyAdminBindableServices` currently returns an empty list.
- `docs/en/rip2-proxy-admin-m1-public-api-draft.proto` is documentation-only and is not compiled.

Submission gap:

- Taiyi RIP-2 requires public admin gRPC service definitions and at least `ListClients` / `DescribeClient` implemented through the official public endpoint.
- This branch currently lacks generated `apache.rocketmq.v2.ProxyAdminServiceGrpc` and therefore cannot expose a real public `ProxyAdminService`.

## File Structure

External repo, required for the formal API gate:

- Modify: `../rocketmq-apis/apache/rocketmq/v2/admin.proto`
  - Add standalone `ProxyAdminService`, request/response messages, `ProxyScope`, and `ProxyClient`.
- Build/install locally from: `../rocketmq-apis/java/BUILD.bazel`
  - Produce a local `org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` artifact for this repo to compile against.

Current repo:

- Modify: `pom.xml`
  - Temporarily point `<rocketmq-proto.version>` to `2.2.0-rip2-SNAPSHOT` for contest verification until an official artifact is published.
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java`
  - Thin generated-proto service that delegates every public RPC to `ProxyClientAdminEndpointExecutor`.
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
  - Register `GrpcProxyAdminApplication` in the admin gRPC server by default when `enableProxyAdminGrpcServer=true`.
- Create: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java`
  - Real generated gRPC service test using an actual in-process server/channel.
- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java`
  - Assert the default admin service factory now returns `GrpcProxyAdminApplication`.
- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminWiringTest.java`
  - Assert the default activity exposes the endpoint executor needed by the public service.
- Modify: `docs/en/rip2-proxy-admin-m1-public-api-draft.proto`
  - Keep it byte-for-byte aligned with the final `rocketmq-apis` proto, marked as a mirror.
- Modify: `docs/en/rip2-proxy-admin-m1-submission-package.md`
- Modify: `docs/cn/rip2-proxy-admin-m1-submission-package.md`
  - Refresh commit, verification command, test result, proto gate status, and final submission checklist.
- Create: `docs/en/rip2-proxy-admin-m1-final-smoke.md`
- Create: `docs/cn/rip2-proxy-admin-m1-final-smoke.md`
  - Add exact manual startup and `grpcurl` commands for the public endpoint.

## Task 1: Baseline Guard

**Files:**
- Read only: current repository

- [x] **Step 1: Confirm branch, upstream delta, and clean worktree**

Run:

```bash
cd .
git status --short --branch --untracked-files=all
git log --oneline --decorate --max-count=8
git rev-list --count origin/develop..HEAD
git diff --check
```

Expected:

```text
## rip2-proxy-admin-m1...origin/rip2-proxy-admin-m1
```

Expected `git diff --check`: no output.

- [x] **Step 2: Re-run the existing broad proxy verification before changing code**

Run:

```bash
cd .
mvn -pl proxy -am "-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" -DfailIfNoTests=false test -DskipITs
```

Expected:

```text
Tests run: 700, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

JDK 17 may print JaCoCo 0.8.5 instrumentation stack traces. Treat them as noise only when Surefire reports zero failures/errors and Maven exits successfully.

- [x] **Step 3: Commit only if baseline docs have drifted**

Run:

```bash
cd .
git status --short --untracked-files=all
```

Expected: clean. If not clean because test tools generated untracked files, inspect them and delete only generated test artifacts. Do not revert user code.

## Task 2: Formalize the Public API in `rocketmq-apis`

**Files:**
- Create if missing: `../rocketmq-apis`
- Modify: `../rocketmq-apis/apache/rocketmq/v2/admin.proto`
- Read: `../rocketmq-apis/java/BUILD.bazel`
- Read: `../rocketmq-apis/java/VERSION`

- [x] **Step 1: Prepare the sibling `rocketmq-apis` checkout**

Run:

```bash
cd ..
if [ ! -d rocketmq-apis/.git ]; then
  git clone https://github.com/apache/rocketmq-apis.git rocketmq-apis
fi
cd rocketmq-apis
git fetch origin
git checkout main
git pull --ff-only origin main
git checkout -B rip2-proxy-admin-public-api
```

Expected:

```text
Switched to a new branch 'rip2-proxy-admin-public-api'
```

or:

```text
Reset branch 'rip2-proxy-admin-public-api'
```

- [x] **Step 2: Replace `apache/rocketmq/v2/admin.proto` with the formal M1 API**

Edit `../rocketmq-apis/apache/rocketmq/v2/admin.proto` so the complete file is:

```proto
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

syntax = "proto3";

package apache.rocketmq.v2;

option cc_enable_arenas = true;
option csharp_namespace = "Apache.Rocketmq.V2";
option java_multiple_files = true;
option java_package = "apache.rocketmq.v2";
option java_generate_equals_and_hash = true;
option java_string_check_utf8 = true;
option java_outer_classname = "MQAdmin";

message ChangeLogLevelRequest {
  enum Level {
    TRACE = 0;
    DEBUG = 1;
    INFO = 2;
    WARN = 3;
    ERROR = 4;
  }
  Level level = 1;
}

message ChangeLogLevelResponse { string remark = 1; }

service Admin {
  rpc ChangeLogLevel(ChangeLogLevelRequest) returns (ChangeLogLevelResponse) {}
}

// ProxyAdminService is the public Proxy admin interface. M1 exposes online
// client query APIs. Future modules can add proxy config, quota, route,
// connection, and diagnosis APIs without mixing with the data-plane
// MessagingService.
service ProxyAdminService {
  rpc ListClients(ListClientsRequest) returns (ListClientsResponse) {}
  rpc DescribeClient(DescribeClientRequest) returns (DescribeClientResponse) {}
  rpc ListClientsByGroup(ListClientsByGroupRequest)
      returns (ListClientsByGroupResponse) {}
  rpc ListClientsByTopic(ListClientsByTopicRequest)
      returns (ListClientsByTopicResponse) {}
}

enum ProxyScope {
  PROXY_SCOPE_UNSPECIFIED = 0;
  PROXY_SCOPE_LOCAL_PROXY = 1;
  PROXY_SCOPE_ALL_PROXIES = 2;
  PROXY_SCOPE_PROXY_ID = 3;
}

message ListClientsRequest {
  string client_id = 1;
  string client_id_prefix = 2;
  string group = 3;
  string topic = 4;
  string client_language = 5;
  optional int64 connect_time_start_millis = 6;
  optional int64 connect_time_end_millis = 7;
  int32 page_num = 8;
  int32 page_size = 9;
  ProxyScope scope = 10;
  string proxy_id = 11;
}

message ListClientsResponse {
  Status status = 1;
  repeated ProxyClient clients = 2;
  bool has_more = 3;
}

message DescribeClientRequest {
  string client_id = 1;
  ProxyScope scope = 2;
  string proxy_id = 3;
}

message DescribeClientResponse {
  Status status = 1;
  ProxyClient client = 2;
}

message ListClientsByGroupRequest {
  string group = 1;
  string client_id = 2;
  string client_id_prefix = 3;
  string client_language = 4;
  optional int64 connect_time_start_millis = 5;
  optional int64 connect_time_end_millis = 6;
  int32 page_num = 7;
  int32 page_size = 8;
  ProxyScope scope = 9;
  string proxy_id = 10;
}

message ListClientsByGroupResponse {
  Status status = 1;
  repeated ProxyClient clients = 2;
  bool has_more = 3;
}

message ListClientsByTopicRequest {
  string topic = 1;
  string client_id = 2;
  string client_id_prefix = 3;
  string client_language = 4;
  optional int64 connect_time_start_millis = 5;
  optional int64 connect_time_end_millis = 6;
  int32 page_num = 7;
  int32 page_size = 8;
  ProxyScope scope = 9;
  string proxy_id = 10;
}

message ListClientsByTopicResponse {
  Status status = 1;
  repeated ProxyClient clients = 2;
  bool has_more = 3;
}

message ProxyClient {
  string client_id = 1;
  ClientType client_type = 2;
  repeated string groups = 3;
  repeated string topics = 4;
  string language = 5;
  string remote_address = 6;
  string local_address = 7;
  string version = 8;
  int64 connect_time_millis = 9;
  int64 last_active_time_millis = 10;
  string proxy_id = 11;
}
```

- [x] **Step 3: Verify `rocketmq-apis` still generates Java**

Run:

```bash
cd ../rocketmq-apis
bazel build //java:rocketmq-proto
```

Expected:

```text
INFO: Build completed successfully
```

- [x] **Step 4: Install a local generated artifact for current-repo verification**

Run:

```bash
cd ../rocketmq-apis
bazel build //java:rocketmq-proto
PROTO_JAR="$(find bazel-bin -type f -name '*rocketmq-proto*.jar' | head -n 1)"
test -n "$PROTO_JAR"
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
```

If `PROTO_JAR` is empty, run `find bazel-bin -type f | sort` and use the jar generated by `//java:rocketmq-proto`. Do not proceed until `apache.rocketmq.v2.ProxyAdminServiceGrpc` is inside the installed jar:

```bash
jar tf "$PROTO_JAR" | grep 'apache/rocketmq/v2/ProxyAdminServiceGrpc.class'
```

Expected:

```text
apache/rocketmq/v2/ProxyAdminServiceGrpc.class
```

- [x] **Step 5: Commit the API branch**

Run:

```bash
cd ../rocketmq-apis
git diff --check
git status --short
git add apache/rocketmq/v2/admin.proto
git commit -m "Add proxy admin online client query API"
```

Expected:

```text
[rip2-proxy-admin-public-api ...] Add proxy admin online client query API
```

## Task 3: Upgrade This Repo to the Generated Public API Artifact

**Files:**
- Modify: `pom.xml`
- Modify: `docs/en/rip2-proxy-admin-m1-public-api-draft.proto`

- [x] **Step 1: Update Maven dependency version**

In `./pom.xml`, change:

```xml
<rocketmq-proto.version>2.1.2</rocketmq-proto.version>
```

to:

```xml
<rocketmq-proto.version>2.2.0-rip2-SNAPSHOT</rocketmq-proto.version>
```

- [x] **Step 2: Verify generated symbols are visible to the proxy module**

Run:

```bash
cd .
mvn -pl proxy -am -DskipTests compile
```

Expected:

```text
BUILD SUCCESS
```

- [x] **Step 3: Mirror the final public proto into documentation**

Copy the complete `rocketmq-apis/apache/rocketmq/v2/admin.proto` content from Task 2 into:

```text
./docs/en/rip2-proxy-admin-m1-public-api-draft.proto
```

Keep this extra comment immediately above `syntax = "proto3";`:

```proto
// Documentation mirror for the RIP-2 contest branch. The authoritative source
// is apache/rocketmq/v2/admin.proto in the rocketmq-apis branch
// rip2-proxy-admin-public-api.
```

- [x] **Step 4: Commit the dependency and doc mirror**

Run:

```bash
cd .
git diff --check
git add pom.xml docs/en/rip2-proxy-admin-m1-public-api-draft.proto
git commit -m "Use RIP-2 proxy admin public API artifact"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Use RIP-2 proxy admin public API artifact
```

## Task 4: Add the Generated-Proto Public gRPC Application

**Files:**
- Create: `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java`
- Create: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java`

- [x] **Step 1: Write the failing public gRPC service test**

Create `./proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyClient;
import apache.rocketmq.v2.ProxyScope;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GrpcProxyAdminApplicationTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
    }

    @Test
    public void listAndDescribeClientsThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PUSH_CONSUMER, "group-b", "topic-b"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse listResponse = stub.listClients(ListClientsRequest.newBuilder()
                .setPageNum(1)
                .setPageSize(100)
                .build());

            assertThat(listResponse.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(listResponse.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a", "client-b");
            assertThat(listResponse.getHasMore()).isFalse();

            DescribeClientResponse describeResponse = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("client-a")
                .build());

            assertThat(describeResponse.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(describeResponse.getClient().getClientId()).isEqualTo("client-a");
            assertThat(describeResponse.getClient().getClientType()).isEqualTo(ClientType.PRODUCER);
            assertThat(describeResponse.getClient().getGroupsList()).containsExactly("group-a");
            assertThat(describeResponse.getClient().getTopicsList()).containsExactly("topic-a");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void publicServiceRejectsNonLocalM1Scope() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse response = stub.listClients(ListClientsRequest.newBuilder()
                .setScope(ProxyScope.PROXY_SCOPE_ALL_PROXIES)
                .setPageNum(1)
                .setPageSize(100)
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(response.getStatus().getMessage()).contains("only supports LOCAL_PROXY");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    private static ProxyClientReadService readService(DefaultGrpcMessagingActivity activity) throws Exception {
        Field field = DefaultGrpcMessagingActivity.class.getDeclaredField("proxyClientReadService");
        field.setAccessible(true);
        return (ProxyClientReadService) field.get(activity);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String group, String topic) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            Collections.singleton(group),
            Collections.singleton(topic),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }
}
```

- [x] **Step 2: Run the new test and verify the expected failure**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=GrpcProxyAdminApplicationTest test -DskipITs
```

Expected failure:

```text
cannot find symbol
  symbol:   class GrpcProxyAdminApplication
```

If the failure is about `ProxyAdminServiceGrpc`, go back to Task 2 and reinstall the generated `rocketmq-proto` artifact.

- [x] **Step 3: Implement `GrpcProxyAdminApplication`**

Create `./proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java`:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyClient;
import apache.rocketmq.v2.ProxyScope;
import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase {
    private final ProxyClientAdminEndpointExecutor endpointExecutor;
    private final ProxyClientAdminRequestConverter requestConverter;

    public GrpcProxyAdminApplication(ProxyClientAdminEndpointExecutor endpointExecutor) {
        if (endpointExecutor == null) {
            throw new IllegalArgumentException("endpointExecutor is required");
        }
        this.endpointExecutor = endpointExecutor;
        this.requestConverter = ProxyClientAdminRequestConverter.getInstance();
    }

    @Override
    public void listClients(ListClientsRequest request, StreamObserver<ListClientsResponse> responseObserver) {
        this.endpointExecutor.listClients(
            request,
            this::toListClientsRequest,
            responseObserver,
            this::toListClientsResponse
        );
    }

    @Override
    public void describeClient(DescribeClientRequest request,
        StreamObserver<DescribeClientResponse> responseObserver) {
        this.endpointExecutor.describeClient(
            request,
            this::toDescribeClientRequest,
            responseObserver,
            this::toDescribeClientResponse
        );
    }

    @Override
    public void listClientsByGroup(ListClientsByGroupRequest request,
        StreamObserver<ListClientsByGroupResponse> responseObserver) {
        this.endpointExecutor.listClientsByGroup(
            request,
            this::toListClientsByGroupRequest,
            responseObserver,
            this::toListClientsByGroupResponse
        );
    }

    @Override
    public void listClientsByTopic(ListClientsByTopicRequest request,
        StreamObserver<ListClientsByTopicResponse> responseObserver) {
        this.endpointExecutor.listClientsByTopic(
            request,
            this::toListClientsByTopicRequest,
            responseObserver,
            this::toListClientsByTopicResponse
        );
    }

    private ProxyClientAdminListClientsRequest toListClientsRequest(ListClientsRequest request) {
        return this.requestConverter.toListClientsRequest(
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getGroup(),
            request.getTopic(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminDescribeClientRequest toDescribeClientRequest(DescribeClientRequest request) {
        return this.requestConverter.toDescribeClientRequest(
            request.getClientId(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminListClientsByGroupRequest toListClientsByGroupRequest(ListClientsByGroupRequest request) {
        return this.requestConverter.toListClientsByGroupRequest(
            request.getGroup(),
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminListClientsByTopicRequest toListClientsByTopicRequest(ListClientsByTopicRequest request) {
        return this.requestConverter.toListClientsByTopicRequest(
            request.getTopic(),
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ListClientsResponse toListClientsResponse(Status status, ProxyClientAdminPageView pageView) {
        ListClientsResponse.Builder builder = ListClientsResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private DescribeClientResponse toDescribeClientResponse(Status status, ProxyClientAdminClientView clientView) {
        DescribeClientResponse.Builder builder = DescribeClientResponse.newBuilder().setStatus(status);
        if (clientView != null) {
            builder.setClient(toProxyClient(clientView));
        }
        return builder.build();
    }

    private ListClientsByGroupResponse toListClientsByGroupResponse(Status status,
        ProxyClientAdminPageView pageView) {
        ListClientsByGroupResponse.Builder builder = ListClientsByGroupResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private ListClientsByTopicResponse toListClientsByTopicResponse(Status status,
        ProxyClientAdminPageView pageView) {
        ListClientsByTopicResponse.Builder builder = ListClientsByTopicResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private static List<ProxyClient> toProxyClients(List<ProxyClientAdminClientView> clientViews) {
        List<ProxyClient> clients = new ArrayList<>(clientViews.size());
        for (ProxyClientAdminClientView clientView : clientViews) {
            clients.add(toProxyClient(clientView));
        }
        return clients;
    }

    private static ProxyClient toProxyClient(ProxyClientAdminClientView clientView) {
        return ProxyClient.newBuilder()
            .setClientId(clientView.getClientId())
            .setClientType(clientView.getClientType())
            .addAllGroups(clientView.getGroups())
            .addAllTopics(clientView.getTopics())
            .setLanguage(clientView.getLanguage())
            .setRemoteAddress(clientView.getRemoteAddress())
            .setLocalAddress(clientView.getLocalAddress())
            .setVersion(clientView.getClientVersion())
            .setConnectTimeMillis(clientView.getConnectTimeMillis())
            .setLastActiveTimeMillis(clientView.getLastActiveTimeMillis())
            .setProxyId(clientView.getProxyId())
            .build();
    }

    private static Long optionalLong(boolean present, long value) {
        return present ? value : null;
    }

    private static int pageNumOrDefault(int pageNum) {
        if (pageNum == 0) {
            return 1;
        }
        return pageNum;
    }

    private static String scopeName(ProxyScope scope) {
        if (scope == null) {
            return ProxyScope.PROXY_SCOPE_UNSPECIFIED.name();
        }
        return scope.name();
    }
}
```

- [x] **Step 4: Run the new test and verify it passes**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=GrpcProxyAdminApplicationTest test -DskipITs
```

Expected:

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [x] **Step 5: Commit public gRPC application**

Run:

```bash
cd .
git diff --check
git add proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java \
  proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java
git commit -m "Add RIP-2 public proxy admin gRPC service"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Add RIP-2 public proxy admin gRPC service
```

## Task 5: Wire the Public Service Into Proxy Startup

**Files:**
- Modify: `proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`
- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java`
- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminWiringTest.java`

- [x] **Step 1: Add failing startup tests**

In `./proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java`, add imports:

```java
import org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplication;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminEndpointExecutor;
```

Add this test near the existing proxy-admin startup tests:

```java
    @Test
    public void testCreateProxyAdminGrpcBindableServicesRegistersPublicProxyAdminServiceByDefault() throws Exception {
        CommandLineArgument commandLineArgument = ProxyStartup.parseCommandLineArgument(new String[] {
            "-pm", "cluster"
        });
        ProxyStartup.initConfiguration(commandLineArgument);
        DefaultGrpcMessagingActivity sharedActivity = mock(DefaultGrpcMessagingActivity.class);
        ProxyClientAdminEndpointExecutor endpointExecutor = mock(ProxyClientAdminEndpointExecutor.class);
        Mockito.when(sharedActivity.getProxyClientAdminEndpointExecutor()).thenReturn(endpointExecutor);

        List<BindableService> services = ProxyStartup.createProxyAdminGrpcBindableServices(sharedActivity);

        assertEquals(1, services.size());
        Assert.assertTrue(services.get(0) instanceof GrpcProxyAdminApplication);
    }
```

In `./proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminWiringTest.java`, add this test:

```java
    @Test
    public void createDefaultActivityExposesAdminEndpointExecutorForPublicGrpcService() {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);

        try {
            ProxyClientAdminEndpointExecutor endpointExecutor = activity.getProxyClientAdminEndpointExecutor();

            assertThat(endpointExecutor).isNotNull();
        } finally {
            activity.shutdown();
        }
    }
```

- [x] **Step 2: Run tests and verify startup failure**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=ProxyStartupTest#testCreateProxyAdminGrpcBindableServicesRegistersPublicProxyAdminServiceByDefault,GrpcProxyAdminWiringTest#createDefaultActivityExposesAdminEndpointExecutorForPublicGrpcService test -DskipITs
```

Expected failure:

```text
Expected :1
Actual   :0
```

for the default proxy admin bindable services test.

- [x] **Step 3: Register the public service by default**

In `./proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java`, add import:

```java
import org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplication;
```

Replace:

```java
    private static List<BindableService> createProxyAdminBindableServices(
        DefaultGrpcMessagingActivity grpcMessagingActivity) {
        return Lists.newArrayList();
    }
```

with:

```java
    private static List<BindableService> createProxyAdminBindableServices(
        DefaultGrpcMessagingActivity grpcMessagingActivity) {
        DefaultGrpcMessagingActivity requiredGrpcMessagingActivity = requireGrpcMessagingActivity(grpcMessagingActivity);
        return Lists.newArrayList(new GrpcProxyAdminApplication(
            requiredGrpcMessagingActivity.getProxyClientAdminEndpointExecutor()
        ));
    }
```

- [x] **Step 4: Run startup and wiring tests**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=ProxyStartupTest,GrpcProxyAdminWiringTest,GrpcProxyAdminApplicationTest test -DskipITs
```

Expected:

```text
Failures: 0, Errors: 0
BUILD SUCCESS
```

- [x] **Step 5: Commit startup wiring**

Run:

```bash
cd .
git diff --check
git add proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java \
  proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java \
  proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminWiringTest.java
git commit -m "Wire RIP-2 proxy admin service into startup"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Wire RIP-2 proxy admin service into startup
```

## Task 6: Add Public Endpoint Coverage for All Four RPCs

**Files:**
- Modify: `proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java`

- [x] **Step 1: Add tests for `ListClientsByGroup`, `ListClientsByTopic`, default page number, optional connect time, and not found**

Append these test methods to `GrpcProxyAdminApplicationTest`:

```java
    @Test
    public void listClientsByGroupAndTopicThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PUSH_CONSUMER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PRODUCER, "group-b", "topic-b"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            assertThat(stub.listClientsByGroup(apache.rocketmq.v2.ListClientsByGroupRequest.newBuilder()
                .setGroup("group-a")
                .setPageSize(100)
                .build()).getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a");

            assertThat(stub.listClientsByTopic(apache.rocketmq.v2.ListClientsByTopicRequest.newBuilder()
                .setTopic("topic-b")
                .setPageSize(100)
                .build()).getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-b");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void listClientsDefaultsPageNumAndHonorsOptionalConnectTime() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PRODUCER, "group-a", "topic-a"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse response = stub.listClients(ListClientsRequest.newBuilder()
                .setConnectTimeStartMillis(100)
                .setConnectTimeEndMillis(100)
                .setPageSize(100)
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(response.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a", "client-b");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void describeMissingClientReturnsNotFoundStatusWithoutClientBodyThroughGeneratedGrpcService()
        throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            DescribeClientResponse response = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("missing-client")
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
            assertThat(response.getStatus().getMessage()).contains("Client not found: missing-client");
            assertThat(response.hasClient()).isFalse();
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }
```

- [x] **Step 2: Run public service tests**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=GrpcProxyAdminApplicationTest test -DskipITs
```

Expected:

```text
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

- [x] **Step 3: Commit public endpoint coverage**

Run:

```bash
cd .
git diff --check
git add proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java
git commit -m "Cover RIP-2 public proxy admin RPCs"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Cover RIP-2 public proxy admin RPCs
```

## Task 7: Update Submission Documentation and Smoke Commands

**Files:**
- Modify: `docs/en/rip2-proxy-admin-m1-submission-package.md`
- Modify: `docs/cn/rip2-proxy-admin-m1-submission-package.md`
- Create: `docs/en/rip2-proxy-admin-m1-final-smoke.md`
- Create: `docs/cn/rip2-proxy-admin-m1-final-smoke.md`

- [x] **Step 1: Add English smoke guide**

Create `./docs/en/rip2-proxy-admin-m1-final-smoke.md`:

```markdown
# RIP-2 Proxy Admin M1 Final Smoke

This smoke verifies the generated public `apache.rocketmq.v2.ProxyAdminService`
endpoint after the `rocketmq-apis` branch has been built and
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` has been installed
locally.

## Build

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

Expected:

```text
BUILD SUCCESS
```

## Start Proxy With Public Admin Server

Use a normal local or cluster Proxy config and set:

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

Then start Proxy with the same command used by the existing RocketMQ
deployment. The public admin service is registered only on the admin gRPC
server, not on the data-plane `MessagingService` server.

## Manual grpcurl Calls

List clients:

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Describe one client:

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

List clients by group:

```bash
grpcurl -plaintext \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

List clients by topic:

```bash
grpcurl -plaintext \
  -d '{"topic":"topic-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByTopic
```

Expected successful response shape:

```json
{
  "status": {
    "code": "OK"
  },
  "clients": [],
  "hasMore": false
}
```

`client-a`, `group-a`, and `topic-a` are sample values. Use IDs that are online
in the target cluster during manual smoke. `clients` may be empty when no gRPC
clients are online. The unit and integration tests seed synthetic clients to
validate non-empty responses.

## M1 Scope Rule

The public M1 endpoint supports omitted scope and
`PROXY_SCOPE_LOCAL_PROXY`. It rejects `PROXY_SCOPE_ALL_PROXIES` and
`PROXY_SCOPE_PROXY_ID` until the community finalizes multi-proxy discovery,
authorization, timeout, and page ownership semantics.
```

- [x] **Step 2: Add Chinese smoke guide**

Create `./docs/cn/rip2-proxy-admin-m1-final-smoke.md`:

```markdown
# RIP-2 Proxy Admin M1 最终冒烟

本冒烟用于验证生成版公开 `apache.rocketmq.v2.ProxyAdminService` 端点。
执行前需要先完成 `rocketmq-apis` 分支构建，并在本机安装
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`。

## 构建

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

预期：

```text
BUILD SUCCESS
```

## 启动公开 admin gRPC server

使用常规 local 或 cluster Proxy 配置，并设置：

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

然后按现有 RocketMQ 部署方式启动 Proxy。公开 admin service 只注册在
admin gRPC server 上，不注册到数据面 `MessagingService` server。

## grpcurl 手工调用

查询客户端列表：

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

查询单个客户端：

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

按 group 查询：

```bash
grpcurl -plaintext \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

按 topic 查询：

```bash
grpcurl -plaintext \
  -d '{"topic":"topic-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByTopic
```

成功响应形态：

```json
{
  "status": {
    "code": "OK"
  },
  "clients": [],
  "hasMore": false
}
```

`client-a`、`group-a`、`topic-a` 是样例值。手工冒烟时使用目标集群中真实在线的
ID。如果当前没有在线 gRPC 客户端，`clients` 可以为空。单元测试和集成测试会注入
synthetic clients 来验证非空响应。

## M1 scope 规则

公开 M1 端点只支持省略 scope 或 `PROXY_SCOPE_LOCAL_PROXY`。
`PROXY_SCOPE_ALL_PROXIES` 和 `PROXY_SCOPE_PROXY_ID` 会被拒绝，直到社区确认
多 Proxy discovery、鉴权、超时和分页归属语义。
```

- [x] **Step 3: Refresh submission package status**

In both submission package files, update the current status section so it says:

```markdown
This branch now contains the generated public `ProxyAdminService` endpoint wiring.
The authoritative protobuf source is the linked `rocketmq-apis`
`rip2-proxy-admin-public-api` branch. For contest verification, the generated
Java artifact is installed locally as
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`.
```

Chinese version:

```markdown
本分支已包含生成版公开 `ProxyAdminService` endpoint 接线。权威 protobuf
来源是配套的 `rocketmq-apis` `rip2-proxy-admin-public-api` 分支。为了完成
参赛验证，生成版 Java artifact 以
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` 安装到本机 Maven。
```

Update the requirement table rows:

```markdown
| public admin service，包含 `ListClients`、`DescribeClient`、`ListClientsByGroup`、`ListClientsByTopic` | 已通过生成版 `ProxyAdminServiceGrpc` 暴露，默认注册到独立 admin gRPC server。 | `GrpcProxyAdminApplication`、`ProxyStartup`、`GrpcProxyAdminApplicationTest`、`rocketmq-apis/apache/rocketmq/v2/admin.proto`。 |
```

Update verification section after Task 8 with the actual command and result.

- [x] **Step 4: Commit docs**

Run:

```bash
cd .
git diff --check
git add docs/en/rip2-proxy-admin-m1-submission-package.md \
  docs/cn/rip2-proxy-admin-m1-submission-package.md \
  docs/en/rip2-proxy-admin-m1-final-smoke.md \
  docs/cn/rip2-proxy-admin-m1-final-smoke.md
git commit -m "Refresh RIP-2 final submission evidence"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Refresh RIP-2 final submission evidence
```

## Task 8: Final Verification

**Files:**
- Read only except documentation timestamp updates from Task 7

- [x] **Step 1: Run focused public API tests**

Run:

```bash
cd .
mvn -pl proxy -am -Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest test -DskipITs
```

Expected:

```text
Failures: 0, Errors: 0
BUILD SUCCESS
```

- [x] **Step 2: Run broad proxy verification**

Run:

```bash
cd .
mvn -pl proxy -am "-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" -DfailIfNoTests=false test -DskipITs
```

Expected:

```text
Failures: 0, Errors: 0
BUILD SUCCESS
```

Record the exact Surefire summary in both submission package docs.

- [x] **Step 3: Run compile/package smoke**

Run:

```bash
cd .
mvn -pl proxy -am -DskipTests package -DskipITs
```

Expected:

```text
BUILD SUCCESS
```

- [x] **Step 4: Run static whitespace checks**

Run:

```bash
cd .
git diff --check origin/develop...HEAD
git diff --check
```

Expected: no output.

- [x] **Step 5: Confirm final worktree and commit graph**

Run:

```bash
cd .
git status --short --branch --untracked-files=all
git log --oneline --decorate --max-count=12
```

Expected:

```text
## rip2-proxy-admin-m1...origin/rip2-proxy-admin-m1
```

If commits are ahead of origin, push only if environment policy allows public GitHub pushes:

```bash
git push origin rip2-proxy-admin-m1
```

If public push is not allowed, stop and report the local branch and commit hash instead.

## Task 9: Final Contest Submission Package

**Files:**
- Modify if verification numbers changed: `docs/en/rip2-proxy-admin-m1-submission-package.md`
- Modify if verification numbers changed: `docs/cn/rip2-proxy-admin-m1-submission-package.md`

- [x] **Step 1: Prepare the submission summary**

Generate the Taiyi submission or PR description with this command:

```bash
cd .
ROCKETMQ_COMMIT="$(git rev-parse HEAD)"
APIS_COMMIT="$(git -C ../rocketmq-apis rev-parse HEAD)"
mkdir -p target
cat > target/rip2-proxy-admin-submission.md <<EOF
## RIP-2 Proxy Admin M1 Submission

This submission implements the RocketMQ RIP-2 Proxy Admin M1 online client query module.

### Repositories / commits

- rocketmq branch: `rip2-proxy-admin-m1`
- rocketmq commit: `${ROCKETMQ_COMMIT}`
- rocketmq-apis branch: `rip2-proxy-admin-public-api`
- rocketmq-apis commit: `${APIS_COMMIT}`

### Implemented M1 APIs

- `apache.rocketmq.v2.ProxyAdminService/ListClients`
- `apache.rocketmq.v2.ProxyAdminService/DescribeClient`
- `apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup`
- `apache.rocketmq.v2.ProxyAdminService/ListClientsByTopic`

### Scope

M1 public endpoint supports local proxy query semantics. `PROXY_SCOPE_ALL_PROXIES`
and `PROXY_SCOPE_PROXY_ID` are implemented as internal explorations but remain
gated from the public endpoint until the community finalizes multi-proxy
discovery, timeout, authorization, and page-token ownership semantics.

### Verification

- Public gRPC service tests pass: `GrpcProxyAdminApplicationTest`
- Startup wiring tests pass: `ProxyStartupTest`, `GrpcProxyAdminWiringTest`
- Broad proxy admin verification passes with zero failures/errors
- 1M synthetic client benchmark remains documented with local read-model P99 < 1s

### Notes

The authoritative protobuf source is the companion `rocketmq-apis` branch. For
local contest verification, the generated Java artifact is installed as
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`.
EOF
cat target/rip2-proxy-admin-submission.md
```

Expected: `target/rip2-proxy-admin-submission.md` contains concrete commit hashes, not shell variables.

- [x] **Step 2: Final acceptance checklist**

Confirm every line is true before declaring the project submit-ready:

```text
[x] rocketmq-apis has formal ProxyAdminService proto in apache/rocketmq/v2/admin.proto.
[x] Local Maven has org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT with ProxyAdminServiceGrpc.
[x] Current rocketmq branch compiles against generated ProxyAdminServiceGrpc.
[x] GrpcProxyAdminApplication delegates all four public RPCs.
[x] ProxyStartup registers GrpcProxyAdminApplication on the independent admin gRPC server.
[x] Public generated gRPC service tests pass through a real Server/ManagedChannel.
[x] Existing endpoint-ready, ACL, metrics, lifecycle, and read-model tests still pass.
[x] Submission docs include final commit hashes and exact verification output.
[x] Worktree is clean or only contains intentionally uncommitted submission notes.
```

- [x] **Step 3: Commit any final doc-only hash updates**

Run:

```bash
cd .
git status --short --untracked-files=all
git add docs/en/rip2-proxy-admin-m1-submission-package.md \
  docs/cn/rip2-proxy-admin-m1-submission-package.md
git commit -m "Finalize RIP-2 contest submission package"
```

Expected:

```text
[rip2-proxy-admin-m1 ...] Finalize RIP-2 contest submission package
```

If there are no doc changes, `git commit` may say there is nothing to commit. In that case, do not create an empty commit.

## Self-Review Notes

Spec coverage:

- Public admin gRPC service: Task 2 defines proto, Tasks 4-5 expose it.
- `ListClients` / `DescribeClient`: Task 4 tests and implements both.
- `ListClientsByGroup` / `ListClientsByTopic`: Task 6 tests all four public RPCs.
- Formal proto/service definition: Task 2 puts the API into `rocketmq-apis/apache/rocketmq/v2/admin.proto`.
- Multi-proxy semantics: public endpoint stays `LOCAL_PROXY`; docs explicitly gate future scopes.
- ACL 2.0 path: existing `AuthorizingClientAdminService` remains in the delegated endpoint chain; broad verification keeps auth tests.
- P99 < 1s benchmark: existing benchmark doc remains part of the submission package.
- OpenTelemetry / observability: existing metered service and broad verification remain required.

No task should reimplement the read model, lifecycle hooks, ACL policy, or internal cross-proxy coordinator. Those are already present and covered. The plan only closes the official public API and submission gap.

## Post-Plan Community Review Artifacts

- [x] Public API draft PR opened: https://github.com/apache/rocketmq-apis/pull/112
- [x] RocketMQ implementation draft PR opened: https://github.com/apache/rocketmq/pull/10603
- [x] RIP-2 tracking issue summary posted: https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687
- [x] Generated public gRPC endpoint 1M benchmark added and documented:
  `GrpcProxyAdminApplicationBenchmark` reports worst public endpoint P99 at
  3.576 ms on 1,000,000 synthetic clients.
- [x] GitHub review artifacts refreshed after the public endpoint benchmark:
  Apache RocketMQ PR #10603, rocketmq-apis PR #112, and the RIP-2 issue
  summary now reference implementation-code checkpoint
  `7b89ba60fca2a18859519f7b2b822f73c2f4ed2c`, broad verification
  `Tests run: 731`, and public endpoint P99 `3.576 ms`.
- [x] Dashboard CLIENT-01 field-level handoff contract added:
  `docs/en/rip2-proxy-admin-m1-dashboard-contract.md` and
  `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md` map `ProxyClient` fields,
  example `DescribeClient` output, pagination, errors, and M1 scope limits for
  external RIP-1 Dashboard E2E.
- [x] Dashboard table-field generated gRPC coverage added:
  `GrpcProxyAdminApplicationTest#listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService`
  directly verifies `ListClients` returns Dashboard table fields, complementing
  the existing `DescribeClient` detail-field coverage.
- [x] Public service descriptor generated gRPC coverage added:
  `GrpcProxyAdminApplicationTest#bindServiceExposesGeneratedProxyAdminUnaryMethods`
  fixes the generated service name `apache.rocketmq.v2.ProxyAdminService`,
  all four public method names, and unary method descriptors.
- [x] Submission package head evidence refreshed:
  `docs/en/rip2-proxy-admin-m1-submission-package.md` and
  `docs/cn/rip2-proxy-admin-m1-submission-package.md` now distinguish the
  latest synchronized implementation-code checkpoint
  `6a267c1a483379bd1c934ceeb9b49a6f99fc5f63`
  from the earlier 1M benchmark code checkpoint `7b89ba60fca2a18859519f7b2b822f73c2f4ed2c`.
- [x] Focused public endpoint/startup verification evidence refreshed after the
  generated public service descriptor test:
  `Tests run: 55, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`,
  finished at `2026-07-10T09:00:08+08:00`.
- [x] Broad proxy admin verification evidence refreshed on the latest branch
  head after descriptor and Dashboard-facing tests were included in the broad
  suite: `Tests run: 731, Failures: 0, Errors: 0, Skipped: 0`,
  `BUILD SUCCESS`, finished at `2026-07-10T09:01:07+08:00`.
- [x] Lightweight submission guard added:
  `dev/rip2_submission_guard.py` checks required RIP-2 files, the local
  `rocketmq-proto:2.2.0-rip2-SNAPSHOT` generated artifact, proto mirror
  alignment with `../rocketmq-apis`, focused/broad verification evidence, and
  optional git remote state. Unit coverage lives in
  `dev/rip2_submission_guard_test.py`.
- [x] GitHub review artifact guard added:
  `python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github`
  verifies the RocketMQ fork branch, companion rocketmq-apis proposal branch
  through its configured upstream remote, RocketMQ draft PR, rocketmq-apis draft
  PR, and RIP-2 issue comment all reference the current RocketMQ and
  rocketmq-apis heads and include submission-guard evidence.
- [x] Plan execution checkbox guard added:
  `dev/rip2_submission_guard.py` now checks this execution plan for unfinished
  task checkboxes so future final-submission guard runs fail if plan progress
  tracking drifts from the completed checkpoint state.
- [x] Public review artifact link guard added:
  `dev/rip2_submission_guard.py` now requires the local submission evidence to
  keep the RocketMQ draft PR, rocketmq-apis draft PR, and RIP-2 issue-comment
  links visible for reviewers.
- [x] Public PR metadata guard added:
  `dev/rip2_submission_guard.py --check-github` now verifies the RocketMQ and
  rocketmq-apis review PRs remain open draft PRs from the expected
  `pilichoumao` branches into the expected Apache base branches.
- [x] Public PR feedback guard added:
  `dev/rip2_submission_guard.py --check-github` now fails when either review
  PR has comments or reviews, making new community feedback visible before a
  final contest submission package is reported as current.
- [x] RIP-2 issue feedback guard added:
  `dev/rip2_submission_guard.py --check-github` now verifies the RIP-2 tracking
  issue remains open, still matches the Proxy Admin title, and has no comments
  after the published submission-summary comment.
- [x] Public PR checks guard added:
  `dev/rip2_submission_guard.py --check-github` now treats absent GitHub checks
  as acceptable for the current draft review branches, but fails if reported PR
  checks appear and any check is not passing or explicitly skipped.
- [x] Maven verification evidence refreshed after guard hardening:
  focused public endpoint/startup verification, broad proxy admin verification,
  and package smoke were rerun with Temurin JDK 17.0.18 after the reviewer
  guard hardening checkpoints, and the submission evidence timestamps were
  refreshed accordingly.
- [x] Local `rocketmq-proto` snapshot metadata guard added:
  `dev/rip2_submission_guard.py` now verifies the installed
  `org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` artifact includes
  the expected jar, pom, local Maven metadata, and repository marker files, in
  addition to generated public RPC classes.
- [x] rocketmq-apis Java build metadata guard added:
  `dev/rip2_submission_guard.py` now verifies the companion API proposal keeps
  `java/VERSION` at `2.2.0` and retains the Bazel `rocketmq-proto`,
  `assemble-maven`, and `deploy-maven` targets needed to regenerate and publish
  the proposed public proto artifact.
- [x] Package smoke refreshed after the submission guard:
  `mvn -pl proxy -am -DskipTests package -DskipITs` completed with
  `BUILD SUCCESS`, finished at `2026-07-10T09:02:11+08:00`.
- [x] Generated public endpoint coverage guard added:
  `dev/rip2_submission_guard.py` now verifies that the generated public gRPC
  endpoint tests still cover the service descriptor, success paths, contest
  filters, pagination, non-local M1 scope gate, authorization failure, not
  found semantics, Dashboard-facing fields, and startup/admin-server isolation.
- [x] Generated public endpoint internal-error coverage added:
  `GrpcProxyAdminApplicationTest#publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService`
  verifies that an unexpected endpoint-layer failure is returned as a public
  response `INTERNAL_SERVER_ERROR` status instead of escaping as a transport
  error; the submission guard now requires this coverage.
- [x] Internal-error public endpoint evidence synchronized into reviewer docs:
  after adding the public `INTERNAL_SERVER_ERROR` generated gRPC coverage, the
  focused public endpoint/startup suite was rerun with
  `Tests run: 55, Failures: 0, Errors: 0, Skipped: 0`, the broad proxy admin
  suite was rerun with
  `Tests run: 731, Failures: 0, Errors: 0, Skipped: 0`, and package smoke was
  rerun with `BUILD SUCCESS`, finished at `2026-07-10T09:02:11+08:00`.
- [x] Generated public endpoint NOT_FOUND body-contract coverage tightened:
  the submission guard was first made to fail when
  `GrpcProxyAdminApplicationTest#describeMissingClientReturnsNotFoundStatusWithoutClientBodyThroughGeneratedGrpcService`
  was absent; the generated gRPC test now verifies `DescribeClient` returns a
  public `NOT_FOUND` status/message for an offline client without a
  `ProxyClient` result body. The focused method verification passed with
  `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`,
  finished at `2026-07-10T08:43:47+08:00`.
- [x] Generated public endpoint BAD_REQUEST and UNAUTHORIZED body-contract
  coverage tightened: the submission guard was first made to fail when
  `GrpcProxyAdminApplicationTest#publicServiceMapsBadRequestResponsesWithoutResultBodiesThroughGeneratedGrpcService`
  and
  `GrpcProxyAdminApplicationTest#publicServiceMapsUnauthorizedResponsesWithoutResultBodiesThroughGeneratedGrpcService`
  were absent; the existing generated gRPC status/body assertions were renamed
  to those explicit contract names, and the focused method verification passed
  with `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`,
  finished at `2026-07-10T08:58:23+08:00`. Focused public endpoint/startup
  verification, broad proxy admin verification, and package smoke were then
  refreshed at `2026-07-10T09:00:08+08:00`,
  `2026-07-10T09:01:07+08:00`, and `2026-07-10T09:02:11+08:00`.
- [x] M1 public scope gate evidence guard added:
  `dev/rip2_submission_guard.py` now requires the local submission evidence and
  every GitHub review artifact to retain `PROXY_SCOPE_LOCAL_PROXY`,
  `PROXY_SCOPE_ALL_PROXIES`, and `PROXY_SCOPE_PROXY_ID`, so the accepted local
  scope and the two gated multi-proxy scopes remain visible to reviewers.
- [x] Final verification and core coverage evidence refreshed after scope-guard
  hardening: focused verification passed with `Tests run: 55` at
  `2026-07-10T09:49:05+08:00`, broad verification passed with
  `Tests run: 731` at `2026-07-10T09:50:12+08:00`, and package smoke passed at
  `2026-07-10T09:51:17+08:00`. Fresh JaCoCo aggregation keeps instruction,
  branch, and line coverage above 85% for both RIP-2 core packages.
- [x] Public pre-service failure metrics gap closed: request adapter/context
  failures and query-executor rejection are now recorded before service
  invocation, while delegated requests remain service-metered to avoid double
  counting. The submission guard pins the three executor boundary tests and
  production wiring test. Focused verification passed with `Tests run: 56` at
  `2026-07-10T10:11:44+08:00`, broad verification passed with
  `Tests run: 735` at `2026-07-10T10:13:01+08:00`, and package smoke passed at
  `2026-07-10T10:14:37+08:00`.
- [x] Public admin request-boundary hardening completed: the endpoint executor
  now propagates gRPC and OpenTelemetry contexts across the dedicated query
  executor and distinguishes task admission from inline task failure to avoid
  duplicate endpoint metrics. Authenticated usernames, transport-derived
  addresses, and the complete `proxy_protocol_*` namespace replace or clear
  client-supplied metadata. The stricter subject policy is scoped to public
  admin so messaging keeps its existing whitelist behavior; the public admin
  pipeline no longer trusts a raw subject header when authentication is
  disabled, and the admin front executor uses `AbortPolicy` so saturation is
  observable instead
  of silently discarding an RPC. The submission guard pins these trust-boundary
  and context tests. Focused verification passed with `Tests run: 56` at
  `2026-07-11T18:41:53+08:00`, expanded broad verification passed with
  `Tests run: 763` at `2026-07-11T23:49:46+08:00`, and package smoke passed at
  `2026-07-11T23:51:22+08:00`.
- [x] Production-interceptor dual-server authentication and isolation E2E
  completed. `GrpcProxyAdminProductionInterceptorE2ETest` starts independent
  messaging and admin loopback servers through production
  `GrpcServerBuilder.configInterceptor()`, verifies service isolation in both
  directions, replaces forged subject/address metadata with authenticated and
  transport-derived values, proves authentication failure bypasses ACL, and
  maps authenticated ACL denial to `UNAUTHORIZED`. The combined regression
  suite passed with `Tests run: 63` and `BUILD SUCCESS` at
  `2026-07-11T19:24:15+08:00`; the expanded broad suite then passed with
  `Tests run: 763` at `2026-07-11T23:49:46+08:00`, followed by package smoke at
  `2026-07-11T23:51:22+08:00`.
- [x] Constrained-heap 1M proof completed. Performance TDD first reproduced
  broad-prefix P99 `8388.608 ms`, combined-filter P99 `13505.659 ms`, and deep
  page P99 `1157.313 ms`; the read model now uses a live prefix range view,
  page-bounded index intersection, and mutation-invalidated page anchors. The
  benchmark harness also uses production-shaped 4-thread/10,000-queue server
  and query executors. Clean benchmark verification passed with `Tests run: 38`
  and `BUILD SUCCESS` at `2026-07-11T22:26:00+08:00`. Under a 4 GiB fixed heap,
  final P99 values were broad prefix `137.526 ms`, combined filters
  `243.610 ms`, deep page 10000 `0.016 ms`, and generated public gRPC combined
  filters `29.042 ms`; maximum JFR heapUsed was `1126.4 MiB`, maximum RSS was
  `1283.0 MiB`, and all runs completed with zero swaps and no OOM. Final broad
  verification includes the benchmark harness tests and passed with
  `Tests run: 763` at
  `2026-07-11T23:49:46+08:00`, followed by package smoke at
  `2026-07-11T23:51:22+08:00`. Final review also hardened query-executor
  teardown with a forced-shutdown fallback and made the submission guard check
  the constrained-heap evidence independently in both benchmark reports.
- [x] Public admin production exposure and server lifecycle hardening completed.
  The independent admin listener now registers only
  `GrpcProxyAdminApplication`; server reflection, Channelz, and the unauthenticated
  internal peer service remain off that listener pending trusted proxy identity
  semantics. `GrpcServer` now owns and closes its Netty boss/worker event loops,
  forces the gRPC server down after a bounded wait, and the admin query executor
  also forces shutdown after timeout. Smoke commands explicitly load
  `../rocketmq-apis/apache/rocketmq/v2/admin.proto` and include reproducible
  authenticated metadata. The resource/isolation suite passed with
  `Tests run: 71`, guard unit tests passed `51/51`, broad verification passed
  with `Tests run: 763, Failures: 0, Errors: 0, Skipped: 0` at
  `2026-07-11T23:49:46+08:00`, and package smoke passed at
  `2026-07-11T23:51:22+08:00`.
- [x] Post-submission GitHub evidence audit completed. The public PR bodies and
  RIP-2 issue summary must carry the final package-level JaCoCo values for both
  core packages; the submission guard rejects missing current coverage and the
  stale pre-performance `88.01% / 95.66%` service-package values.

The RocketMQ implementation draft PR is intentionally marked draft because it
depends on the local contest artifact
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`, generated from the
companion `rocketmq-apis` proposal branch. It should remain downstream of the
API ownership and artifact publication decision.

- [x] Wide connect-time range materialization removed and verified. Multi-bucket
  time queries now drive the smallest existing ordered index and validate time
  per candidate instead of constructing a temporary union. Regression coverage
  pins pagination, token validation, selective-index reuse, read-model JMH, and
  generated public gRPC JMH. The 1M-client, 100-bucket, fixed-4-GiB runs measured
  read-model P99 `0.010 ms` at `1113.352 B/op` and generated gRPC P99 `0.394 ms`
  at `174970.776 B/op`; both completed with zero swaps and no OOM. Final broad
  verification passed with `Tests run: 767, Failures: 0, Errors: 0, Skipped: 0`
  at `2026-07-12T00:28:33+08:00`, followed by package smoke `BUILD SUCCESS` at
  `2026-07-12T00:30:07+08:00`.
- [x] Generated artifact reproducibility evidence hardened. Reviewer-facing EN/CN
  submission and smoke docs now pin companion API commit
  `c372905ce927cf8957333e7ac07877f295fd7ec9` and the installed
  `rocketmq-proto` jar SHA-256; the submission guard computes the local digest
  and rejects stale or missing documentation evidence.
- [x] Filtered and full-range deep pagination hardened. Ordered connect-time
  buckets use a bounded merge for partial ranges, while a range covering the
  complete time index reuses mutation-invalidated client-id page anchors.
  Checkpoints `f07395462`, `226d2bf07`, and `4a086b543` preserve stable pages
  without materializing million-client unions.
- [x] Queued public admin cancellation hardened at checkpoint `a7c4ecfac`.
  `ProxyClientAdminEndpointExecutor` re-checks gRPC cancellation and expired
  deadlines before request conversion or service work, maps deadlines to
  `DEADLINE_EXCEEDED`, and avoids recording client cancellation as a service
  failure.
- [x] gRPC client lifecycle ownership hardened at checkpoint `1dd5c6fd1`.
  Stream generations, transport identity, and striped lifecycle locks prevent
  stale reconnect callbacks, heartbeats, terminations, and unregister events
  from mutating a newer active session; producer settings validate atomically.
- [x] Repository-owned benchmark evidence runner and strict CI gate added.
  `dev/run_rip2_benchmark.sh` captures build, environment, command, classpath,
  JMH JSON/log, JFR, GC, process-time, and SHA-256 evidence. The final 1M deep
  full-range runs at evidence checkpoint `8c3098d51` measured read-model P99
  `0.011 ms` and generated public gRPC P99 `0.843 ms`, with zero swaps and no
  OOM. The review runbook pins both
  repository commits, while `--require-github-checks` remains the explicit
  external release gate until Apache CI reports checks.
- [x] Final verification refreshed after lifecycle, cancellation, pagination,
  and evidence-runner hardening. Focused public endpoint/startup verification
  passed with `Tests run: 57, Failures: 0, Errors: 0, Skipped: 0` at
  `2026-07-12T02:37:36+08:00`; broad proxy admin verification passed with
  `Tests run: 779, Failures: 0, Errors: 0, Skipped: 0` at
  `2026-07-12T02:35:18+08:00`; package smoke completed with `BUILD SUCCESS` at
  `2026-07-12T02:36:24+08:00`. Final package coverage remains above 85% for
  instruction, branch, and line metrics in both RIP-2 core packages.
