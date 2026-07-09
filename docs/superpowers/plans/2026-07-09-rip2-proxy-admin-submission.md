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
mvn -pl proxy -am "-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" -DfailIfNoTests=false test -DskipITs
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
    public void describeMissingClientReturnsNotFoundStatus() throws Exception {
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
mvn -pl proxy -am "-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" -DfailIfNoTests=false test -DskipITs
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
  `Tests run: 728`, and public endpoint P99 `3.576 ms`.
- [x] Dashboard CLIENT-01 field-level handoff contract added:
  `docs/en/rip2-proxy-admin-m1-dashboard-contract.md` and
  `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md` map `ProxyClient` fields,
  example `DescribeClient` output, pagination, errors, and M1 scope limits for
  external RIP-1 Dashboard E2E.
- [x] Dashboard table-field generated gRPC coverage added:
  `GrpcProxyAdminApplicationTest#listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService`
  directly verifies `ListClients` returns Dashboard table fields, complementing
  the existing `DescribeClient` detail-field coverage.

The RocketMQ implementation draft PR is intentionally marked draft because it
depends on the local contest artifact
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`, generated from the
companion `rocketmq-apis` proposal branch. It should remain downstream of the
API ownership and artifact publication decision.
