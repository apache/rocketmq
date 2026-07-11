#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tempfile
import unittest
import zipfile
from pathlib import Path

import rip2_submission_guard


PROTO = """syntax = "proto3";
package apache.rocketmq.v2;

service ProxyAdminService {
  rpc ListClients(ListClientsRequest) returns (ListClientsResponse) {}
  rpc DescribeClient(DescribeClientRequest) returns (DescribeClientResponse) {}
  rpc ListClientsByGroup(ListClientsByGroupRequest) returns (ListClientsByGroupResponse) {}
  rpc ListClientsByTopic(ListClientsByTopicRequest) returns (ListClientsByTopicResponse) {}
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
"""


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def create_snapshot_jar(m2_repository):
    jar_path = (
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
        / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.jar"
    )
    jar_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(jar_path, "w") as jar_file:
        for entry in rip2_submission_guard.REQUIRED_JAR_ENTRIES:
            jar_file.writestr(entry, b"")


def create_snapshot_metadata(m2_repository):
    artifact_dir = (
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
    )
    write(
        artifact_dir / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.pom",
        """<project>
  <groupId>org.apache.rocketmq</groupId>
  <artifactId>rocketmq-proto</artifactId>
  <version>2.2.0-rip2-SNAPSHOT</version>
</project>
""",
    )
    write(
        artifact_dir / "maven-metadata-local.xml",
        """<metadata>
  <groupId>org.apache.rocketmq</groupId>
  <artifactId>rocketmq-proto</artifactId>
  <version>2.2.0-rip2-SNAPSHOT</version>
  <localCopy>true</localCopy>
  <extension>jar</extension>
  <extension>pom</extension>
</metadata>
""",
    )
    write(
        artifact_dir / "_remote.repositories",
        "rocketmq-proto-2.2.0-rip2-SNAPSHOT.jar>=\n"
        "rocketmq-proto-2.2.0-rip2-SNAPSHOT.pom>=\n",
    )


def create_snapshot_jar_without(m2_repository, missing_entry):
    jar_path = (
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
        / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.jar"
    )
    with zipfile.ZipFile(jar_path, "w") as jar_file:
        for entry in rip2_submission_guard.REQUIRED_JAR_ENTRIES:
            if entry != missing_entry:
                jar_file.writestr(entry, b"")


def create_submission_tree(root, apis_root, m2_repository):
    write(
        root / "dev/run_rip2_benchmark.sh",
        "\n".join(rip2_submission_guard.REQUIRED_BENCHMARK_RUNNER_TOKENS) + "\n",
    )
    write(
        root / "pom.xml",
        "<rocketmq-proto.version>2.2.0-rip2-SNAPSHOT</rocketmq-proto.version>\n",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java",
        """class GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase {
  void listClients() {}
  void describeClient() {}
  void listClientsByGroup() {}
  void listClientsByTopic() {}
}
""",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java",
        """class ProxyStartup {
  void start() {
    isEnableProxyAdminGrpcServer();
    getProxyAdminGrpcServerPort();
    ProtoReflectionService.newInstance();
  }
  static GrpcServerBuilder configureProxyAdminGrpcServer() {
    new GrpcProxyAdminApplication(null);
    new ThreadPoolExecutor.AbortPolicy();
  }
  void createProxyAdminGrpcBindableServices() {}
}
""",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServer.java",
        "class GrpcServer {}\n",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServerBuilder.java",
        "class GrpcServerBuilder {}\n",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java",
        """class ProxyConfig {
  private boolean enableProxyAdminGrpcServer = false;
  private Integer proxyAdminGrpcServerPort = 8082;
}
""",
    )
    write(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcRequestPipelineFactory.java",
        "class GrpcRequestPipelineFactory {\n"
        "  void createProxyClientAdmin() { AuthenticationPipeline.forProxyAdmin(null, null); }\n"
        "}\n",
    )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
        "class GrpcProxyAdminApplicationTest {\n"
        + "\n".join(
            f"  void {test_name}() {{}}"
            for test_name in rip2_submission_guard.REQUIRED_GENERATED_PUBLIC_ENDPOINT_TESTS
        )
        + "\n}\n",
    )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java",
        "class ProxyStartupTest {\n"
        + "\n".join(
            f"  void {test_name}() {{}}"
            for test_name in rip2_submission_guard.REQUIRED_ADMIN_SERVER_ISOLATION_TESTS
        )
        + "\n}\n",
    )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "GrpcProxyAdminProductionInterceptorE2ETest.java",
        "class GrpcProxyAdminProductionInterceptorE2ETest {\n"
        + "\n".join(
            f"  void {test_name}() {{}}"
            for test_name in rip2_submission_guard.REQUIRED_PRODUCTION_INTERCEPTOR_E2E_TESTS
        )
        + "\n}\n",
    )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "ProxyClientAdminEndpointExecutorTest.java",
        "class ProxyClientAdminEndpointExecutorTest {\n"
        + "\n".join(
            f"  void {test_name}() {{}}"
            for test_name in (
                rip2_submission_guard.REQUIRED_ENDPOINT_FAILURE_METRICS_TESTS
                + rip2_submission_guard.REQUIRED_ENDPOINT_CONTEXT_PROPAGATION_TESTS
                + ("shutdownForcesSuppliedQueryExecutorAfterTimeout",)
            )
        )
        + "\n}\n",
    )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/GrpcServerTest.java",
        "class GrpcServerTest {\n"
        "  void shutdownForcesServerAndClosesOwnedEventLoopsAfterTimeout() {}\n"
        "}\n",
    )
    wide_connect_time_tests = {}
    for path, test_name in rip2_submission_guard.REQUIRED_WIDE_CONNECT_TIME_TESTS:
        wide_connect_time_tests.setdefault(path, []).append(test_name)
    for path, test_names in wide_connect_time_tests.items():
        write(
            root / path,
            "class WideConnectTimeTest {\n"
            + "\n".join(f"  void {test_name}() {{}}" for test_name in test_names)
            + "\n}\n",
        )
    write(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "GrpcProxyAdminWiringTest.java",
        "class GrpcProxyAdminWiringTest {\n"
        + "\n".join(
            f"  void {test_name}() {{}}"
            for test_name in rip2_submission_guard.REQUIRED_ENDPOINT_FAILURE_METRICS_WIRING_TESTS
        )
        + "\n}\n",
    )
    trust_boundary_tests = {}
    for path, test_name in rip2_submission_guard.REQUIRED_ADMIN_REQUEST_TRUST_BOUNDARY_TESTS:
        trust_boundary_tests.setdefault(path, []).append(test_name)
    for path, test_names in trust_boundary_tests.items():
        write(
            root / path,
            "class TrustBoundaryTest {\n"
            + "\n".join(f"  void {test_name}() {{}}" for test_name in test_names)
            + "\n}\n",
        )
    write(root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto", PROTO)
    write(apis_root / "apache/rocketmq/v2/admin.proto", PROTO)
    write(apis_root / "java/VERSION", "2.2.0\n")
    write(
        apis_root / "java/BUILD.bazel",
        """java_library(
    name = "rocketmq-proto",
    tags = ["maven_coordinates=org.apache.rocketmq:rocketmq-proto:{pom_version}"],
)

assemble_maven(
    name = "assemble-maven",
    target = ":rocketmq-proto",
    version_file = ":VERSION",
)

deploy_maven(
    name = "deploy-maven",
    target = ":assemble-maven",
)
""",
    )

    create_snapshot_jar(m2_repository)
    artifact_sha256 = rip2_submission_guard.file_sha256(
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
        / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.jar"
    )
    submission = f"""
{rip2_submission_guard.FOCUSED_RESULT}
{rip2_submission_guard.FOCUSED_FINISHED_AT}
{rip2_submission_guard.BROAD_RESULT}
{rip2_submission_guard.BROAD_FINISHED_AT}
{rip2_submission_guard.PACKAGE_SMOKE_FINISHED_AT}
3.576 ms
0.681 ms
4 GiB fixed heap
137.526 ms
243.610 ms
0.016 ms
29.042 ms
1126.4 MiB
1283.0 MiB
188604.8 B/op
zero swaps
0.843 ms
2916.2 MiB
8c3098d51615189677118200955aeb6bdcbf90c0
81f309ab9559d60772b26f59ec3a1d4de618840f3a4b949a934d8367b1672308
d519f533b3d20e57a9fec0d15dca339ef769da3eae18817ad34b04a1ca91ee91
f58b88d5234c97ea6942968e970cca247b8821e018337418bd68bf2d26ae6975
841a5ec9c6a4059a88b8f4e42714182f9c8ac1e8a2a7ef5461f05c6c5dc09251
Dashboard CLIENT-01
external validation item
official artifact
rocketmq-apis public proto proposal
PROXY_SCOPE_LOCAL_PROXY
PROXY_SCOPE_ALL_PROXIES
PROXY_SCOPE_PROXY_ID
publicServiceMapsBadRequestResponsesWithoutResultBodiesThroughGeneratedGrpcService
publicServiceMapsUnauthorizedResponsesWithoutResultBodiesThroughGeneratedGrpcService
publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService
https://github.com/apache/rocketmq/pull/10603
https://github.com/apache/rocketmq-apis/pull/112
https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687
-import-path ../rocketmq-apis
-proto apache/rocketmq/v2/admin.proto
x-mq-date-time
MQv2-HMAC-SHA1
server reflection
Channelz
internal peer
Finished at: 2026-07-12T02:49:18+08:00
Client not found: offline-smoke-client
public proxy admin endpoint only supports LOCAL_PROXY scope: PROXY_SCOPE_ALL_PROXIES
pageSize must be greater than or equal to 0
Method not found: apache.rocketmq.v2.ProxyAdminService/ListClients
Method not found: apache.rocketmq.v2.MessagingService/QueryRoute
server does not support the reflection API
port-9876-closed
port-8081-closed
port-8082-closed
action_required
maintainer approval
Build and Run Tests by Maven
Build and Run Tests by Bazel
CodeQL Analysis
Coverage
License checker
Misspell Check
Run Integration Tests
CI
command -v grpcurl
command -v openssl
command -v xxd
target/rip2-smoke-rmq-proxy.json
mvn -Prelease-all -DskipTests -DskipITs package
DIST=distribution/target/rocketmq-5.5.0/rocketmq-5.5.0
"$DIST/bin/mqproxy"
git checkout -B rip2-proxy-admin-public-api c372905ce927cf8957333e7ac07877f295fd7ec9
git checkout -B rip2-proxy-admin-m1 8c3098d51615189677118200955aeb6bdcbf90c0
rocketmq-proto jar SHA-256: {artifact_sha256}
"""
    for rel in rip2_submission_guard.REQUIRED_DOCS:
        write(root / rel, submission)
    write(
        root / "docs/superpowers/plans/2026-07-09-rip2-proxy-admin-submission.md",
        "- [x] completed step\n",
    )
    create_snapshot_metadata(m2_repository)


def fake_github_runner(
    expected_head,
    rocketmq_body,
    api_body,
    issue_body,
    apis_head=None,
    rocketmq_metadata=None,
    api_metadata=None,
    rocketmq_feedback=None,
    api_feedback=None,
    issue_metadata=None,
    rocketmq_checks=None,
    api_checks=None,
    rocketmq_workflow_runs=None,
    api_workflow_runs=None,
):
    rocketmq_metadata = rocketmq_metadata or (
        '{"state":"OPEN","isDraft":true,"headRefName":"rip2-proxy-admin-m1",'
        '"baseRefName":"develop","headRepositoryOwner":{"login":"pilichoumao"}}'
    )
    api_metadata = api_metadata or (
        '{"state":"OPEN","isDraft":true,"headRefName":"rip2-proxy-admin-public-api",'
        '"baseRefName":"main","headRepositoryOwner":{"login":"pilichoumao"}}'
    )
    rocketmq_feedback = rocketmq_feedback or '{"comments":[],"reviews":[]}'
    api_feedback = api_feedback or '{"comments":[],"reviews":[]}'
    issue_metadata = issue_metadata or (
        '{"state":"OPEN",'
        '"title":"[RIP-2] Proxy Admin gRPC Interface Surface (M1: Online Client Query Module)",'
        '"comments":[{"url":"https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687"}]}'
    )
    rocketmq_checks = rocketmq_checks or (1, "", "no checks reported on the 'rip2-proxy-admin-m1' branch")
    api_checks = api_checks or (1, "", "no checks reported on the 'rip2-proxy-admin-public-api' branch")
    rocketmq_workflow_runs = rocketmq_workflow_runs or (
        0,
        '{"total_count":0,"workflow_runs":[]}',
        "",
    )
    api_workflow_runs = api_workflow_runs or (
        0,
        '{"total_count":0,"workflow_runs":[]}',
        "",
    )

    def run(args, cwd):
        if args == ["git", "rev-parse", "HEAD"]:
            if Path(cwd).name == "rocketmq-apis":
                return 0, apis_head or expected_head, ""
            return 0, expected_head, ""
        if args == [
            "gh",
            "pr",
            "view",
            "10603",
            "--repo",
            "apache/rocketmq",
            "--json",
            "state,isDraft,headRefName,baseRefName,headRepositoryOwner",
            "--jq",
            ".",
        ]:
            return 0, rocketmq_metadata, ""
        if args == [
            "gh",
            "pr",
            "view",
            "112",
            "--repo",
            "apache/rocketmq-apis",
            "--json",
            "state,isDraft,headRefName,baseRefName,headRepositoryOwner",
            "--jq",
            ".",
        ]:
            return 0, api_metadata, ""
        if args == [
            "gh",
            "pr",
            "view",
            "10603",
            "--repo",
            "apache/rocketmq",
            "--json",
            "comments,reviews",
            "--jq",
            ".",
        ]:
            return 0, rocketmq_feedback, ""
        if args == [
            "gh",
            "pr",
            "view",
            "112",
            "--repo",
            "apache/rocketmq-apis",
            "--json",
            "comments,reviews",
            "--jq",
            ".",
        ]:
            return 0, api_feedback, ""
        if args == [
            "gh",
            "issue",
            "view",
            "10599",
            "--repo",
            "apache/rocketmq",
            "--json",
            "state,title,comments",
            "--jq",
            ".",
        ]:
            return 0, issue_metadata, ""
        if args == [
            "gh",
            "pr",
            "checks",
            "10603",
            "--repo",
            "apache/rocketmq",
            "--json",
            "name,state,bucket,link",
        ]:
            return rocketmq_checks
        if args == [
            "gh",
            "pr",
            "checks",
            "112",
            "--repo",
            "apache/rocketmq-apis",
            "--json",
            "name,state,bucket,link",
        ]:
            return api_checks
        if args == [
            "gh",
            "api",
            "--method",
            "GET",
            "repos/apache/rocketmq/actions/runs",
            "-f",
            "event=pull_request",
            "-f",
            f"head_sha={expected_head}",
        ]:
            return rocketmq_workflow_runs
        if args == [
            "gh",
            "api",
            "--method",
            "GET",
            "repos/apache/rocketmq-apis/actions/runs",
            "-f",
            "event=pull_request",
            "-f",
            f"head_sha={apis_head or expected_head}",
        ]:
            return api_workflow_runs
        if args == [
            "gh",
            "pr",
            "view",
            "10603",
            "--repo",
            "apache/rocketmq",
            "--json",
            "body",
            "--jq",
            ".body",
        ]:
            return 0, rocketmq_body, ""
        if args == [
            "gh",
            "pr",
            "view",
            "112",
            "--repo",
            "apache/rocketmq-apis",
            "--json",
            "body",
            "--jq",
            ".body",
        ]:
            return 0, api_body, ""
        if args == [
            "gh",
            "api",
            "repos/apache/rocketmq/issues/comments/4926996687",
            "--jq",
            ".body",
        ]:
            return 0, issue_body, ""
        return 1, "", "unexpected command: " + " ".join(args)

    return run


def github_body(rocketmq_head, apis_head=None, guard_command=None):
    guard_command = guard_command or rip2_submission_guard.FULL_GUARD_COMMAND
    body = rocketmq_head + "\n"
    if apis_head:
        body += apis_head + "\n"
    body += guard_command + "\n"
    body += "official artifact\n"
    body += "Dashboard CLIENT-01\n"
    body += "org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT\n"
    body += "action_required\n"
    body += "maintainer approval\n"
    body += "PROXY_SCOPE_LOCAL_PROXY\n"
    body += "PROXY_SCOPE_ALL_PROXIES\n"
    body += "PROXY_SCOPE_PROXY_ID\n"
    body += "service.admin.client instruction 92.93%, branch 86.62%, line 94.41%\n"
    body += "grpc.v2.admin instruction 92.81%, branch 85.79%, line 94.73%\n"
    body += "RIP-2 submission guard passed.\n"
    return body


def fake_apis_git_runner(branch, status, head, remote_head):
    def run(args, cwd):
        if args == ["git", "branch", "--show-current"]:
            return 0, branch, ""
        if args == ["git", "status", "--short", "--untracked-files=all"]:
            return 0, status, ""
        if args == ["git", "rev-parse", "HEAD"]:
            return 0, head, ""
        if args == [
            "git",
            "ls-remote",
            "fork",
            "refs/heads/rip2-proxy-admin-public-api",
        ]:
            return 0, f"{remote_head}\trefs/heads/rip2-proxy-admin-public-api", ""
        return 1, "", "unexpected command: " + " ".join(args)

    return run


def fake_apis_upstream_git_runner(upstream, head, remote_head):
    def run(args, cwd):
        if args == ["git", "branch", "--show-current"]:
            return 0, "rip2-proxy-admin-public-api", ""
        if args == ["git", "status", "--short", "--untracked-files=all"]:
            return 0, "", ""
        if args == ["git", "rev-parse", "HEAD"]:
            return 0, head, ""
        if args == ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]:
            return 0, upstream, ""
        if args == [
            "git",
            "ls-remote",
            "origin",
            "refs/heads/rip2-proxy-admin-public-api",
        ]:
            return 0, f"{remote_head}\trefs/heads/rip2-proxy-admin-public-api", ""
        return 1, "", "unexpected command: " + " ".join(args)

    return run


class Rip2SubmissionGuardTest(unittest.TestCase):
    def test_guard_passes_when_required_submission_evidence_is_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertEqual([], errors)

    def test_guard_reports_stale_broad_verification_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace(
                        rip2_submission_guard.BROAD_RESULT,
                        rip2_submission_guard.PRE_ENDPOINT_METRICS_BROAD_RESULT,
                    ),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("broad verification" in error for error in errors))

    def test_guard_reports_pre_coverage_refresh_broad_verification_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            package_path = root / "docs/en/rip2-proxy-admin-m1-submission-package.md"
            package_path.write_text(
                package_path.read_text(encoding="utf-8")
                + "\nFinished at: 2026-07-10T09:01:07+08:00\n",
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("stale broad verification evidence" in error for error in errors))

    def test_guard_reports_missing_constrained_heap_benchmark_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                write(path, path.read_text().replace("137.526 ms", ""))

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("constrained heap benchmark evidence" in error for error in errors)
            )

    def test_guard_reports_incomplete_reproducible_benchmark_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(root / "dev/run_rip2_benchmark.sh", "#!/usr/bin/env bash\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("reproducible benchmark runner" in error for error in errors))

    def test_guard_requires_constrained_heap_evidence_in_each_bilingual_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            chinese_report = root / "docs/cn/rip2-proxy-admin-m1-benchmark-report.md"
            write(chinese_report, chinese_report.read_text().replace("137.526 ms", ""))

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any(
                    "docs/cn/rip2-proxy-admin-m1-benchmark-report.md" in error
                    and "137.526 ms" in error
                    for error in errors
                )
            )

    def test_guard_reports_pom_proto_version_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(root / "pom.xml", "<rocketmq-proto.version>2.1.2</rocketmq-proto.version>\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("rocketmq-proto.version" in error for error in errors))

    def test_guard_reports_generated_artifact_missing_public_rpc_classes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            create_snapshot_jar_without(
                m2_repository,
                "apache/rocketmq/v2/DescribeClientResponse.class",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("DescribeClientResponse.class" in error for error in errors),
                "guard should require generated public RPC response classes",
            )

    def test_guard_reports_generated_artifact_missing_local_maven_pom(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            (
                m2_repository
                / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
                / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.pom"
            ).unlink()

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("generated rocketmq-proto artifact missing pom" in error for error in errors))

    def test_guard_reports_missing_generated_artifact_sha256_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            artifact_sha256 = rip2_submission_guard.file_sha256(
                m2_repository
                / "org/apache/rocketmq/rocketmq-proto/2.2.0-rip2-SNAPSHOT"
                / "rocketmq-proto-2.2.0-rip2-SNAPSHOT.jar"
            )
            package_path = root / rip2_submission_guard.ARTIFACT_SHA_EVIDENCE_DOCS[0]
            package_path.write_text(
                package_path.read_text(encoding="utf-8").replace(artifact_sha256, "missing"),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("generated artifact SHA-256 evidence" in error for error in errors))

    def test_guard_reports_stale_package_smoke_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "docs/en/rip2-proxy-admin-m1-final-smoke.md",
                "Finished at: 2026-07-10T04:07:14+08:00\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("stale package smoke evidence" in error for error in errors))

    def test_guard_reports_pre_coverage_refresh_package_smoke_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            smoke_path = root / "docs/en/rip2-proxy-admin-m1-final-smoke.md"
            smoke_path.write_text(
                smoke_path.read_text(encoding="utf-8")
                + "\nFinished at: 2026-07-10T09:02:11+08:00\n",
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("stale package smoke evidence" in error for error in errors))

    def test_guard_reports_missing_latest_package_smoke_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace(
                        f"{rip2_submission_guard.PACKAGE_SMOKE_FINISHED_AT}\n",
                        "",
                    ),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("package smoke evidence" in error for error in errors))

    def test_guard_reports_missing_internal_error_public_endpoint_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace(
                        "publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService",
                        "",
                    ),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("INTERNAL_SERVER_ERROR public endpoint evidence" in error for error in errors))

    def test_guard_reports_missing_external_api_artifact_gate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace("official artifact", ""),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("official artifact" in error for error in errors))

    def test_guard_reports_missing_public_scope_gate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace("PROXY_SCOPE_ALL_PROXIES\n", ""),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("public scope gate" in error and "PROXY_SCOPE_ALL_PROXIES" in error for error in errors)
            )

    def test_guard_reports_proto_mirror_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(apis_root / "apache/rocketmq/v2/admin.proto", PROTO + "\nmessage Drift {}\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("public API draft proto mirror" in error for error in errors))

    def test_guard_reports_apis_java_version_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(apis_root / "java/VERSION", "2.1.2\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("rocketmq-apis java/VERSION" in error for error in errors))

    def test_guard_reports_proto_missing_public_rpc_response_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            incomplete_proto = PROTO.replace("message ListClientsByGroupResponse", "message MissingListClientsByGroupResponse")
            write(root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto", incomplete_proto)
            write(apis_root / "apache/rocketmq/v2/admin.proto", incomplete_proto)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("ListClientsByGroupResponse" in error for error in errors))

    def test_guard_reports_proto_missing_proxy_scope_enum_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            incomplete_proto = PROTO.replace("  PROXY_SCOPE_ALL_PROXIES = 2;\n", "")
            write(root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto", incomplete_proto)
            write(apis_root / "apache/rocketmq/v2/admin.proto", incomplete_proto)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("PROXY_SCOPE_ALL_PROXIES = 2" in error for error in errors))

    def test_guard_reports_proto_missing_public_field_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            incomplete_proto = PROTO.replace("  string client_id_prefix = 2;\n", "")
            write(root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto", incomplete_proto)
            write(apis_root / "apache/rocketmq/v2/admin.proto", incomplete_proto)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("string client_id_prefix = 2" in error for error in errors))

    def test_guard_reports_missing_public_admin_startup_wiring(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(root / "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java", "class ProxyStartup {}\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("ProxyStartup" in error and "GrpcProxyAdminApplication" in error for error in errors))

    def test_guard_reports_unbalanced_required_markdown_fences(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(root / "docs/en/rip2-proxy-admin-m1-submission-package.md", "```bash\nunterminated\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("unbalanced markdown code fences" in error for error in errors))

    def test_guard_can_verify_public_github_review_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, body, body, body, apis_head=apis_head),
            )

            self.assertEqual([], errors)

    def test_guard_reports_github_artifact_missing_full_guard_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(
                expected_head,
                apis_head,
                guard_command="python3 dev/rip2_submission_guard.py --check-remote --check-github",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, body, body, body, apis_head=apis_head),
            )

            self.assertTrue(any("full submission guard command" in error for error in errors))

    def test_guard_reports_github_artifact_missing_current_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            good_body = github_body(expected_head, apis_head)
            stale_body = github_body("stale-head", apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, good_body, stale_body, good_body, apis_head=apis_head),
            )

            self.assertTrue(any("rocketmq-apis PR #112" in error and "current HEAD" in error for error in errors))

    def test_guard_reports_github_artifact_missing_external_gate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)
            missing_gate_body = body.replace("official artifact\n", "")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, missing_gate_body, body, body,
                    apis_head=apis_head),
            )

            self.assertTrue(
                any("RocketMQ PR #10603" in error and "official artifact" in error for error in errors)
            )

    def test_guard_reports_github_artifact_missing_public_scope_gate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)
            missing_scope_body = body.replace("PROXY_SCOPE_LOCAL_PROXY\n", "")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, body, body, missing_scope_body,
                    apis_head=apis_head),
            )

            self.assertTrue(
                any("RIP-2 issue comment" in error and "PROXY_SCOPE_LOCAL_PROXY" in error for error in errors)
            )

    def test_guard_reports_stale_github_coverage_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)
            stale_body = body.replace(
                "service.admin.client instruction 92.93%, branch 86.62%, line 94.41%",
                "service.admin.client instruction 93.14%, branch 88.01%, line 95.66%",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    stale_body,
                    body,
                    body,
                    apis_head=apis_head,
                ),
            )

            self.assertTrue(
                any("RocketMQ PR #10603" in error and "coverage evidence" in error for error in errors)
            )

    def test_guard_reports_stale_implementation_checkpoint_in_github_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            stale_body = (
                github_body(expected_head, apis_head)
                + rip2_submission_guard.STALE_IMPLEMENTATION_CHECKPOINTS[0]
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(expected_head, stale_body, stale_body, stale_body,
                    apis_head=apis_head),
            )

            self.assertTrue(any("stale implementation checkpoint" in error for error in errors))

    def test_guard_can_verify_apis_branch_remote_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_apis_remote=True,
                apis_remote="fork",
                command_runner=fake_apis_git_runner(
                    "rip2-proxy-admin-public-api",
                    "",
                    "apis-head",
                    "apis-head",
                ),
            )

            self.assertEqual([], errors)

    def test_guard_auto_detects_apis_upstream_remote(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_apis_remote=True,
                command_runner=fake_apis_upstream_git_runner(
                    "origin/rip2-proxy-admin-public-api",
                    "apis-head",
                    "apis-head",
                ),
            )

            self.assertEqual([], errors)

    def test_guard_reports_apis_remote_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_apis_remote=True,
                apis_remote="fork",
                command_runner=fake_apis_git_runner(
                    "rip2-proxy-admin-public-api",
                    "",
                    "local-apis-head",
                    "remote-apis-head",
                ),
            )

            self.assertTrue(any("fork/rip2-proxy-admin-public-api" in error for error in errors))

    def test_guard_reports_github_artifact_missing_apis_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            good_body = github_body(expected_head, apis_head)
            missing_apis_body = github_body(expected_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    good_body,
                    good_body,
                    missing_apis_body,
                    apis_head=apis_head,
                ),
            )

            self.assertTrue(any("RIP-2 issue comment" in error and "rocketmq-apis HEAD" in error for error in errors))

    def test_guard_reports_unexpected_github_pr_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    rocketmq_metadata=(
                        '{"state":"CLOSED","isDraft":false,"headRefName":"other",'
                        '"baseRefName":"main","headRepositoryOwner":{"login":"someone-else"}}'
                    ),
                ),
            )

            self.assertTrue(any("RocketMQ PR #10603 metadata" in error for error in errors))

    def test_guard_reports_unreviewed_github_pr_feedback(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    rocketmq_feedback='{"comments":[{"body":"please update docs"}],"reviews":[]}',
                ),
            )

            self.assertTrue(any("RocketMQ PR #10603 has unreviewed feedback" in error for error in errors))

    def test_guard_reports_issue_comment_after_submission_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    issue_metadata=(
                        '{"state":"OPEN",'
                        '"title":"[RIP-2] Proxy Admin gRPC Interface Surface (M1: Online Client Query Module)",'
                        '"comments":['
                        '{"url":"https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687"},'
                        '{"url":"https://github.com/apache/rocketmq/issues/10599#issuecomment-5000000000"}'
                        ']}'
                    ),
                ),
            )

            self.assertTrue(any("RIP-2 issue has comments after submission summary" in error for error in errors))

    def test_guard_reports_failed_github_pr_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    rocketmq_checks=(
                        1,
                        '[{"name":"ci","state":"FAILURE","bucket":"fail","link":"https://example.invalid/ci"}]',
                        "",
                    ),
                ),
            )

            self.assertTrue(any("RocketMQ PR #10603 has non-passing check" in error for error in errors))

    def test_guard_strict_ci_gate_reports_missing_github_pr_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                require_github_checks=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                ),
            )

            self.assertTrue(any("RocketMQ PR #10603 has no reported checks" in error for error in errors))
            self.assertTrue(any("rocketmq-apis PR #112 has no reported checks" in error for error in errors))

    def test_guard_strict_ci_gate_reports_workflows_awaiting_maintainer_approval(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                require_github_checks=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    rocketmq_workflow_runs=(
                        0,
                        '{"total_count":2,"workflow_runs":['
                        '{"name":"Build and Run Tests by Maven","conclusion":"action_required"},'
                        '{"name":"License checker","conclusion":"action_required"}]}',
                        "",
                    ),
                    api_workflow_runs=(
                        0,
                        '{"total_count":1,"workflow_runs":['
                        '{"name":"CI","conclusion":"action_required"}]}',
                        "",
                    ),
                ),
            )

            self.assertTrue(
                any(
                    "RocketMQ PR #10603 workflows require maintainer approval" in error
                    and "Build and Run Tests by Maven" in error
                    and "License checker" in error
                    for error in errors
                )
            )
            self.assertTrue(
                any(
                    "rocketmq-apis PR #112 workflows require maintainer approval" in error
                    and "CI" in error
                    for error in errors
                )
            )

    def test_guard_strict_ci_gate_rejects_empty_check_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            expected_head = "abc123"
            apis_head = "apis456"
            body = github_body(expected_head, apis_head)

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
                check_github=True,
                require_github_checks=True,
                command_runner=fake_github_runner(
                    expected_head,
                    body,
                    body,
                    body,
                    apis_head=apis_head,
                    rocketmq_checks=(0, "[]", ""),
                    api_checks=(0, "[]", ""),
                ),
            )

            self.assertTrue(any("RocketMQ PR #10603 has no reported checks" in error for error in errors))
            self.assertTrue(any("rocketmq-apis PR #112 has no reported checks" in error for error in errors))

    def test_guard_parses_strict_ci_gate_option(self):
        args = rip2_submission_guard.parse_args(["--check-github", "--require-github-checks"])

        self.assertTrue(args.check_github)
        self.assertTrue(args.require_github_checks)

    def test_guard_reports_unfinished_plan_checkbox(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "docs/superpowers/plans/2026-07-09-rip2-proxy-admin-submission.md",
                "- [x] completed step\n- [ ] unfinished step\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("unfinished plan checkbox" in error for error in errors))

    def test_guard_reports_stale_implementation_checkpoint_in_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "docs/superpowers/plans/2026-07-09-rip2-proxy-admin-submission.md",
                "- [x] completed step\n"
                f"{rip2_submission_guard.STALE_IMPLEMENTATION_CHECKPOINTS[0]}\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("stale implementation checkpoint" in error for error in errors))

    def test_guard_reports_missing_public_review_artifact_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            for rel in rip2_submission_guard.REQUIRED_DOCS:
                path = root / rel
                path.write_text(
                    path.read_text(encoding="utf-8").replace(
                        "https://github.com/apache/rocketmq/pull/10603\n",
                        "",
                    ),
                    encoding="utf-8",
                )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("public review artifact link" in error for error in errors))

    def test_guard_reports_missing_generated_public_endpoint_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
                "class GrpcProxyAdminApplicationTest {}\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("GrpcProxyAdminApplicationTest" in error and "generated public endpoint" in error
                    for error in errors)
            )

    def test_guard_reports_missing_generated_public_internal_error_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            test_path = root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java"
            test_path.write_text(
                test_path.read_text(encoding="utf-8").replace(
                    "publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService",
                    "missingInternalErrorCoverage",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("GrpcProxyAdminApplicationTest" in error and "INTERNAL_SERVER_ERROR" in error
                    for error in errors)
            )

    def test_guard_reports_missing_endpoint_failure_metrics_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
                "ProxyClientAdminEndpointExecutorTest.java",
                "class ProxyClientAdminEndpointExecutorTest {}\n",
            )
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
                "GrpcProxyAdminWiringTest.java",
                "class GrpcProxyAdminWiringTest {}\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("endpoint failure metrics coverage" in error for error in errors)
            )

    def test_guard_reports_missing_endpoint_context_propagation_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            test_path = (
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
                "ProxyClientAdminEndpointExecutorTest.java"
            )
            test_path.write_text(
                test_path.read_text(encoding="utf-8")
                .replace("listClientsPropagatesGrpcContextToQueryExecutor", "missingGrpcContextCoverage")
                .replace(
                    "listClientsPropagatesOpenTelemetryContextToQueryExecutor",
                    "missingOpenTelemetryContextCoverage",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("endpoint context propagation coverage" in error for error in errors)
            )

    def test_guard_reports_missing_admin_request_trust_boundary_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/"
                "AuthenticationPipelineTest.java",
                "class AuthenticationPipelineTest {}\n",
            )
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/"
                "GrpcRequestPipelineFactoryTest.java",
                "class GrpcRequestPipelineFactoryTest {}\n",
            )
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/"
                "HeaderInterceptorTest.java",
                "class HeaderInterceptorTest {}\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("admin request trust boundary coverage" in error for error in errors)
            )

    def test_guard_reports_missing_admin_executor_abort_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            startup_path = root / "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java"
            startup_path.write_text(
                startup_path.read_text(encoding="utf-8").replace(
                    "new ThreadPoolExecutor.AbortPolicy()",
                    "new ThreadPoolExecutor.DiscardOldestPolicy()",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("ThreadPoolExecutor.AbortPolicy" in error for error in errors))

    def test_guard_reports_missing_strict_admin_authentication_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            factory_path = (
                root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/"
                "GrpcRequestPipelineFactory.java"
            )
            factory_path.write_text(
                factory_path.read_text(encoding="utf-8").replace(
                    "AuthenticationPipeline.forProxyAdmin",
                    "new AuthenticationPipeline",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("strict proxy admin authentication pipeline" in error for error in errors)
            )

    def test_guard_reports_missing_admin_server_isolation_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(root / "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java",
                "class ProxyStartupTest {}\n")

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("ProxyStartupTest" in error and "admin server isolation" in error for error in errors)
            )

    def test_guard_reports_internal_service_exposure_on_public_admin_listener(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            startup_path = root / "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java"
            startup_path.write_text(
                startup_path.read_text(encoding="utf-8").replace(
                    "new GrpcProxyAdminApplication(null);",
                    "new GrpcProxyAdminApplication(null);\n    ProtoReflectionService.newInstance();",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("public admin listener exposes internal service" in error for error in errors))

    def test_guard_reports_missing_bounded_shutdown_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            (root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/GrpcServerTest.java").unlink()

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("bounded shutdown coverage" in error for error in errors))

    def test_guard_reports_missing_wide_connect_time_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            read_service_test = (
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
                "ProxyClientReadServiceTest.java"
            )
            write(
                read_service_test,
                "wideConnectTimeRangeUsesSelectedBucketsWithoutScanningClientIdIndex\n"
                "wideConnectTimeRangeAppliesPageNumToMatchingClients\n"
                "wideConnectTimeRangeRejectsPageTokenOutsideRange\n"
                "wideConnectTimeRangeUsesMoreSelectiveGroupIndex\n",
            )
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
                "ProxyClientReadServiceBenchmarkTest.java",
                "listByWideConnectTimeRangePage\n",
            )
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
                "GrpcProxyAdminApplicationBenchmarkTest.java",
                "listClientsByWideConnectTimeRange\n",
            )
            read_service_test.write_text(
                read_service_test.read_text(encoding="utf-8").replace(
                    "wideConnectTimeRangeUsesSelectedBucketsWithoutScanningClientIdIndex",
                    "missingWideRangeAllocationCoverage",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("wide connect-time coverage" in error for error in errors))

    def test_guard_reports_reflection_dependent_smoke_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            smoke_path = root / "docs/en/rip2-proxy-admin-m1-final-smoke.md"
            smoke_path.write_text(
                smoke_path.read_text(encoding="utf-8").replace(
                    "-proto apache/rocketmq/v2/admin.proto",
                    "",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("manual smoke contract" in error for error in errors))

    def test_guard_reports_missing_live_runtime_smoke_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            smoke_path = root / "docs/en/rip2-proxy-admin-m1-final-smoke.md"
            smoke_path.write_text(
                smoke_path.read_text(encoding="utf-8").replace(
                    "Method not found: apache.rocketmq.v2.ProxyAdminService/ListClients",
                    "",
                ),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("live runtime smoke evidence" in error for error in errors))

    def test_guard_reports_missing_github_actions_approval_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            submission_path = root / "docs/en/rip2-proxy-admin-m1-submission-package.md"
            submission_path.write_text(
                submission_path.read_text(encoding="utf-8").replace("action_required", ""),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("GitHub Actions approval evidence" in error for error in errors))

    def test_guard_reports_missing_smoke_tool_prerequisites(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            smoke_path = root / "docs/en/rip2-proxy-admin-m1-final-smoke.md"
            smoke_path.write_text(
                smoke_path.read_text(encoding="utf-8").replace("command -v grpcurl", ""),
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("command -v grpcurl" in error for error in errors))

    def test_guard_reports_unpinned_review_runbook(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            runbook_path = root / "docs/en/rip2-proxy-admin-m1-review-runbook.md"
            runbook_path.write_text(
                runbook_path.read_text(encoding="utf-8")
                + "\ngit pull --ff-only origin rip2-proxy-admin-m1\n",
                encoding="utf-8",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(any("review runbook must pin commits" in error for error in errors))

    def test_guard_reports_missing_production_interceptor_dual_server_e2e(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            write(
                root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
                "GrpcProxyAdminProductionInterceptorE2ETest.java",
                "class GrpcProxyAdminProductionInterceptorE2ETest {}\n",
            )

            errors = rip2_submission_guard.run_checks(
                root=root,
                apis_root=apis_root,
                m2_repository=m2_repository,
                check_git=False,
                check_remote=False,
            )

            self.assertTrue(
                any("production interceptor dual server E2E" in error for error in errors)
            )


if __name__ == "__main__":
    unittest.main()
