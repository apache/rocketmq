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

"""Local RIP-2 submission guard for reviewer-facing evidence.

The script intentionally checks stable artifacts and recorded verification
evidence. It does not rerun the expensive Maven/JMH commands; reviewers still
use the runbook for those full reproductions.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import zipfile
from pathlib import Path


EXPECTED_BRANCH = "rip2-proxy-admin-m1"
EXPECTED_APIS_BRANCH = "rip2-proxy-admin-public-api"
DEFAULT_APIS_REMOTE = "auto"
PROTO_VERSION = "2.2.0-rip2-SNAPSHOT"
APIS_JAVA_VERSION = "2.2.0"
PROTO_VERSION_XML = f"<rocketmq-proto.version>{PROTO_VERSION}</rocketmq-proto.version>"
FULL_GUARD_COMMAND = "python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github"
PLAN_FILE = "docs/superpowers/plans/2026-07-09-rip2-proxy-admin-submission.md"
RIP2_ISSUE_COMMENT_URL = "https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687"
FOCUSED_RESULT = "Tests run: 56, Failures: 0, Errors: 0, Skipped: 0"
FOCUSED_FINISHED_AT = "Finished at: 2026-07-11T18:41:53+08:00"
BROAD_RESULT = "Tests run: 767, Failures: 0, Errors: 0, Skipped: 0"
BROAD_FINISHED_AT = "Finished at: 2026-07-12T00:28:33+08:00"
PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-12T00:30:07+08:00"
INTERNAL_ERROR_PUBLIC_ENDPOINT_TEST = (
    "publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService"
)
OLD_FOCUSED_RESULT = "Tests run: 52, Failures: 0, Errors: 0, Skipped: 0"
OLD_BROAD_RESULT = "Tests run: 728, Failures: 0, Errors: 0, Skipped: 0"
PREVIOUS_FOCUSED_RESULT = "Tests run: 54, Failures: 0, Errors: 0, Skipped: 0"
PREVIOUS_BROAD_RESULT = "Tests run: 730, Failures: 0, Errors: 0, Skipped: 0"
OLD_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T04:04:54+08:00"
OLD_BROAD_FINISHED_AT = "Finished at: 2026-07-10T05:14:32+08:00"
OLD_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T04:07:14+08:00"
PREVIOUS_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T05:58:49+08:00"
PREVIOUS_BROAD_FINISHED_AT = "Finished at: 2026-07-10T06:06:44+08:00"
PREVIOUS_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T06:27:48+08:00"
RECENT_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T07:52:28+08:00"
RECENT_BROAD_FINISHED_AT = "Finished at: 2026-07-10T07:53:37+08:00"
RECENT_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T07:54:35+08:00"
REFRESHED_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T08:27:13+08:00"
REFRESHED_BROAD_FINISHED_AT = "Finished at: 2026-07-10T08:28:12+08:00"
REFRESHED_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T08:31:23+08:00"
PRE_ERROR_BODY_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T08:45:58+08:00"
PRE_ERROR_BODY_BROAD_FINISHED_AT = "Finished at: 2026-07-10T08:46:56+08:00"
PRE_ERROR_BODY_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T08:47:54+08:00"
PRE_COVERAGE_REFRESH_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T09:00:08+08:00"
PRE_COVERAGE_REFRESH_BROAD_FINISHED_AT = "Finished at: 2026-07-10T09:01:07+08:00"
PRE_COVERAGE_REFRESH_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T09:02:11+08:00"
PRE_ENDPOINT_METRICS_FOCUSED_RESULT = "Tests run: 55, Failures: 0, Errors: 0, Skipped: 0"
PRE_ENDPOINT_METRICS_BROAD_RESULT = "Tests run: 731, Failures: 0, Errors: 0, Skipped: 0"
PRE_ENDPOINT_METRICS_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T09:49:05+08:00"
PRE_ENDPOINT_METRICS_BROAD_FINISHED_AT = "Finished at: 2026-07-10T09:50:12+08:00"
PRE_ENDPOINT_METRICS_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T09:51:17+08:00"
PRE_REQUEST_HARDENING_BROAD_RESULT = "Tests run: 735, Failures: 0, Errors: 0, Skipped: 0"
PRE_REQUEST_HARDENING_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T10:11:44+08:00"
PRE_REQUEST_HARDENING_BROAD_FINISHED_AT = "Finished at: 2026-07-10T10:13:01+08:00"
PRE_REQUEST_HARDENING_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T10:14:37+08:00"
PRE_TRUST_BOUNDARY_BROAD_RESULT = "Tests run: 741, Failures: 0, Errors: 0, Skipped: 0"
PRE_TRUST_BOUNDARY_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T11:55:21+08:00"
PRE_TRUST_BOUNDARY_BROAD_FINISHED_AT = "Finished at: 2026-07-10T11:56:38+08:00"
PRE_TRUST_BOUNDARY_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T11:57:46+08:00"
PRE_BINARY_HEADER_BROAD_RESULT = "Tests run: 748, Failures: 0, Errors: 0, Skipped: 0"
PRE_BINARY_HEADER_BROAD_FINISHED_AT = "Finished at: 2026-07-11T18:46:34+08:00"
PRE_BINARY_HEADER_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-11T18:48:25+08:00"
PRE_PRODUCTION_INTERCEPTOR_BROAD_RESULT = "Tests run: 749, Failures: 0, Errors: 0, Skipped: 0"
PRE_PRODUCTION_INTERCEPTOR_BROAD_FINISHED_AT = "Finished at: 2026-07-11T18:56:24+08:00"
PRE_PRODUCTION_INTERCEPTOR_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-11T18:58:58+08:00"
PRE_CONSTRAINED_HEAP_BROAD_RESULT = "Tests run: 750, Failures: 0, Errors: 0, Skipped: 0"
PRE_CONSTRAINED_HEAP_BROAD_FINISHED_AT = "Finished at: 2026-07-11T19:32:17+08:00"
PRE_CONSTRAINED_HEAP_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-11T19:35:13+08:00"
PRE_TEARDOWN_HARDENING_BROAD_RESULT = "Tests run: 759, Failures: 0, Errors: 0, Skipped: 0"
PRE_TEARDOWN_HARDENING_BROAD_FINISHED_AT = "Finished at: 2026-07-11T22:39:54+08:00"
PRE_TEARDOWN_HARDENING_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-11T22:41:31+08:00"
PRE_ADMIN_EXPOSURE_BROAD_RESULT = "Tests run: 760, Failures: 0, Errors: 0, Skipped: 0"
PRE_ADMIN_EXPOSURE_BROAD_FINISHED_AT = "Finished at: 2026-07-11T23:07:13+08:00"
PRE_ADMIN_EXPOSURE_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-11T23:09:28+08:00"
PUBLIC_REVIEW_ARTIFACT_LINKS = (
    "https://github.com/apache/rocketmq/pull/10603",
    "https://github.com/apache/rocketmq-apis/pull/112",
    RIP2_ISSUE_COMMENT_URL,
)

REQUIRED_EXTERNAL_GATE_TOKENS = (
    "official artifact",
    "Dashboard CLIENT-01",
    "external",
)

REQUIRED_CONSTRAINED_HEAP_EVIDENCE = (
    "4 GiB fixed heap",
    "137.526 ms",
    "243.610 ms",
    "0.016 ms",
    "29.042 ms",
    "1126.4 MiB",
    "1283.0 MiB",
    "188604.8 B/op",
    "zero swaps",
)

CONSTRAINED_HEAP_EVIDENCE_DOCS = (
    "docs/en/rip2-proxy-admin-m1-benchmark-report.md",
    "docs/cn/rip2-proxy-admin-m1-benchmark-report.md",
)

ARTIFACT_SHA_EVIDENCE_DOCS = (
    "docs/en/rip2-proxy-admin-m1-submission-package.md",
    "docs/cn/rip2-proxy-admin-m1-submission-package.md",
    "docs/en/rip2-proxy-admin-m1-final-smoke.md",
    "docs/cn/rip2-proxy-admin-m1-final-smoke.md",
)

GITHUB_EXTERNAL_GATE_TOKENS = (
    "official artifact",
    "Dashboard CLIENT-01",
    "org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT",
)

GITHUB_COVERAGE_EVIDENCE = (
    "service.admin.client instruction 93.14%, branch 86.29%, line 94.59%",
    "grpc.v2.admin instruction 92.76%, branch 85.64%, line 94.67%",
)

STALE_GITHUB_COVERAGE_EVIDENCE = (
    "branch 88.01%, line 95.66%",
)

REQUIRED_PUBLIC_SCOPE_GATE_TOKENS = (
    "PROXY_SCOPE_LOCAL_PROXY",
    "PROXY_SCOPE_ALL_PROXIES",
    "PROXY_SCOPE_PROXY_ID",
)

STALE_IMPLEMENTATION_CHECKPOINTS = (
    "573c716e136845dbd42669d78fd725a18d845435",
)

REQUIRED_DOCS = (
    "docs/en/rip2-proxy-admin-m1-submission-package.md",
    "docs/cn/rip2-proxy-admin-m1-submission-package.md",
    "docs/en/rip2-proxy-admin-m1-review-runbook.md",
    "docs/cn/rip2-proxy-admin-m1-review-runbook.md",
    "docs/en/rip2-proxy-admin-m1-acceptance-audit.md",
    "docs/cn/rip2-proxy-admin-m1-acceptance-audit.md",
    "docs/en/rip2-proxy-admin-m1-final-smoke.md",
    "docs/cn/rip2-proxy-admin-m1-final-smoke.md",
    "docs/en/rip2-proxy-admin-m1-dashboard-contract.md",
    "docs/cn/rip2-proxy-admin-m1-dashboard-contract.md",
    "docs/en/rip2-proxy-admin-m1-benchmark-report.md",
    "docs/cn/rip2-proxy-admin-m1-benchmark-report.md",
    "docs/en/rip2-proxy-admin-public-api-discussion.md",
    "docs/cn/rip2-proxy-admin-public-api-discussion.md",
)

FINAL_SMOKE_DOCS = (
    "docs/en/rip2-proxy-admin-m1-final-smoke.md",
    "docs/cn/rip2-proxy-admin-m1-final-smoke.md",
)

REVIEW_RUNBOOK_DOCS = (
    "docs/en/rip2-proxy-admin-m1-review-runbook.md",
    "docs/cn/rip2-proxy-admin-m1-review-runbook.md",
)

REQUIRED_MANUAL_SMOKE_TOKENS = (
    "command -v grpcurl",
    "command -v openssl",
    "command -v xxd",
    "target/rip2-smoke-rmq-proxy.json",
    "mvn -Prelease-all -DskipTests -DskipITs package",
    "DIST=distribution/target/rocketmq-5.5.0/rocketmq-5.5.0",
    '"$DIST/bin/mqproxy"',
    "-import-path ../rocketmq-apis",
    "-proto apache/rocketmq/v2/admin.proto",
    "x-mq-date-time",
    "MQv2-HMAC-SHA1",
    "server reflection",
    "Channelz",
    "internal peer",
)

REQUIRED_REVIEW_RUNBOOK_TOKENS = (
    "git checkout -B rip2-proxy-admin-public-api c372905ce927cf8957333e7ac07877f295fd7ec9",
    "git checkout -B rip2-proxy-admin-m1 1dd5c6fd1f7e8f5d684213d72c4b965d214f977d",
)

FORBIDDEN_REVIEW_RUNBOOK_TOKENS = (
    "git pull --ff-only origin rip2-proxy-admin-public-api",
    "git pull --ff-only origin rip2-proxy-admin-m1",
)

REQUIRED_BENCHMARK_RUNNER_TOKENS = (
    "target/rip2-benchmark-results/$LABEL",
    "dependency:build-classpath",
    "-XX:StartFlightRecording=filename=",
    "-Xlog:gc*:file=",
    "/usr/bin/time",
    "-prof gc",
    "-rf json",
    "build.log",
    "environment.txt",
    "git rev-parse HEAD",
    '"$JAVA" -version',
    "benchmark source paths are dirty",
    "mvn -pl proxy -am clean",
    "git rev-parse HEAD^{tree}",
    "source-files.txt",
    "runner.sh",
    "SHA256SUMS",
    "JMH include must match exactly one benchmark",
)

REQUIRED_FILES = (
    "pom.xml",
    "dev/run_rip2_benchmark.sh",
    "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java",
    "proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java",
    "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServer.java",
    "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/GrpcServerBuilder.java",
    "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java",
    "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
    "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java",
    "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/GrpcServerTest.java",
    PLAN_FILE,
    "docs/en/rip2-proxy-admin-m1-public-api-draft.proto",
) + REQUIRED_DOCS

REQUIRED_PROTO_MESSAGES = (
    "service ProxyAdminService",
    "enum ProxyScope",
    "message ListClientsRequest",
    "message ListClientsResponse",
    "message DescribeClientRequest",
    "message DescribeClientResponse",
    "message ListClientsByGroupRequest",
    "message ListClientsByGroupResponse",
    "message ListClientsByTopicRequest",
    "message ListClientsByTopicResponse",
    "message ProxyClient",
)

REQUIRED_PROTO_RPC_SIGNATURES = (
    "rpc ListClients ( ListClientsRequest ) returns ( ListClientsResponse )",
    "rpc DescribeClient ( DescribeClientRequest ) returns ( DescribeClientResponse )",
    "rpc ListClientsByGroup ( ListClientsByGroupRequest ) returns ( ListClientsByGroupResponse )",
    "rpc ListClientsByTopic ( ListClientsByTopicRequest ) returns ( ListClientsByTopicResponse )",
)

REQUIRED_PROXY_SCOPE_VALUES = (
    "PROXY_SCOPE_UNSPECIFIED = 0",
    "PROXY_SCOPE_LOCAL_PROXY = 1",
    "PROXY_SCOPE_ALL_PROXIES = 2",
    "PROXY_SCOPE_PROXY_ID = 3",
)

REQUIRED_PROTO_FIELDS = (
    "string client_id = 1",
    "string client_id_prefix = 2",
    "string group = 3",
    "string topic = 4",
    "string client_language = 5",
    "optional int64 connect_time_start_millis = 6",
    "optional int64 connect_time_end_millis = 7",
    "int32 page_num = 8",
    "int32 page_size = 9",
    "ProxyScope scope = 10",
    "string proxy_id = 11",
    "Status status = 1",
    "repeated ProxyClient clients = 2",
    "bool has_more = 3",
    "ProxyClient client = 2",
    "ProxyScope scope = 2",
    "string proxy_id = 3",
    "string group = 1",
    "string client_id = 2",
    "string client_id_prefix = 3",
    "string client_language = 4",
    "optional int64 connect_time_start_millis = 5",
    "optional int64 connect_time_end_millis = 6",
    "int32 page_num = 7",
    "int32 page_size = 8",
    "ProxyScope scope = 9",
    "string proxy_id = 10",
    "string topic = 1",
    "ClientType client_type = 2",
    "repeated string groups = 3",
    "repeated string topics = 4",
    "string language = 5",
    "string remote_address = 6",
    "string local_address = 7",
    "string version = 8",
    "int64 connect_time_millis = 9",
    "int64 last_active_time_millis = 10",
)

REQUIRED_JAR_ENTRIES = (
    "apache/rocketmq/v2/ProxyAdminServiceGrpc.class",
    "apache/rocketmq/v2/ProxyScope.class",
    "apache/rocketmq/v2/ListClientsRequest.class",
    "apache/rocketmq/v2/ListClientsResponse.class",
    "apache/rocketmq/v2/DescribeClientRequest.class",
    "apache/rocketmq/v2/DescribeClientResponse.class",
    "apache/rocketmq/v2/ListClientsByGroupRequest.class",
    "apache/rocketmq/v2/ListClientsByGroupResponse.class",
    "apache/rocketmq/v2/ListClientsByTopicRequest.class",
    "apache/rocketmq/v2/ListClientsByTopicResponse.class",
    "apache/rocketmq/v2/ProxyClient.class",
)

REQUIRED_GENERATED_PUBLIC_ENDPOINT_TESTS = (
    "bindServiceExposesGeneratedProxyAdminUnaryMethods",
    "listAndDescribeClientsThroughGeneratedGrpcService",
    "publicServiceAcceptsExplicitLocalProxyScopeForEveryRpc",
    "publicServiceRejectsNonLocalM1ScopeForEveryRpc",
    "publicServiceRejectsProxyIdM1ScopeForEveryRpc",
    "publicServiceMapsInvalidContestParametersToBadRequest",
    "publicServiceMapsBadRequestResponsesWithoutResultBodiesThroughGeneratedGrpcService",
    "publicServiceMapsUnauthorizedResponsesWithoutResultBodiesThroughGeneratedGrpcService",
    "listClientsHonorsContestFiltersAndPaginationThroughGeneratedGrpcService",
    "listClientsByGroupAndTopicHonorContestFiltersAndPaginationThroughGeneratedGrpcService",
    "describeClientRejectsMissingClientIdThroughGeneratedGrpcService",
    "describeMissingClientReturnsNotFoundStatusWithoutClientBodyThroughGeneratedGrpcService",
    "publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService",
    "listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService",
    "describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService",
)

REQUIRED_ADMIN_SERVER_ISOLATION_TESTS = (
    "testProxyAdminGrpcConfigDefaultsToDisabledIndependentPort",
    "testCreateGrpcBindableServicesDoesNotRegisterAdminPeerService",
    "testCreateProxyAdminGrpcBindableServicesRegistersPublicProxyAdminServiceByDefault",
    "testCreateProxyAdminGrpcBindableServicesDoesNotExposeInternalPeerService",
    "testConfigureProxyAdminGrpcServerRegistersOnlyPublicAdminService",
    "testCreateProxyAdminServerExecutorUsesIndependentThreadNameAndConfig",
)

REQUIRED_PRODUCTION_INTERCEPTOR_E2E_TESTS = (
    "productionInterceptorsAuthenticateAdminAndKeepServicesOnSeparatePorts",
)

REQUIRED_ENDPOINT_FAILURE_METRICS_TESTS = (
    "recordsRejectedQueryExecutorMetricsBeforeServiceInvocation",
    "recordsRequestAdapterFailureMetricsBeforeServiceInvocation",
    "successfulEndpointDelegationDoesNotRecordDuplicateFailureMetrics",
    "delegatedInlineFailureDoesNotRecordEndpointFailureMetrics",
)

REQUIRED_ENDPOINT_CONTEXT_PROPAGATION_TESTS = (
    "listClientsPropagatesGrpcContextToQueryExecutor",
    "listClientsPropagatesOpenTelemetryContextToQueryExecutor",
)

REQUIRED_BOUNDED_SHUTDOWN_TESTS = (
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/GrpcServerTest.java",
        "shutdownForcesServerAndClosesOwnedEventLoopsAfterTimeout",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "ProxyClientAdminEndpointExecutorTest.java",
        "shutdownForcesSuppliedQueryExecutorAfterTimeout",
    ),
)

REQUIRED_WIDE_CONNECT_TIME_TESTS = (
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
        "ProxyClientReadServiceTest.java",
        "wideConnectTimeRangeUsesSelectedBucketsWithoutScanningClientIdIndex",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
        "ProxyClientReadServiceTest.java",
        "wideConnectTimeRangeAppliesPageNumToMatchingClients",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
        "ProxyClientReadServiceTest.java",
        "wideConnectTimeRangeRejectsPageTokenOutsideRange",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
        "ProxyClientReadServiceTest.java",
        "wideConnectTimeRangeUsesMoreSelectiveGroupIndex",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/service/admin/client/"
        "ProxyClientReadServiceBenchmarkTest.java",
        "listByWideConnectTimeRangePage",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "GrpcProxyAdminApplicationBenchmarkTest.java",
        "listClientsByWideConnectTimeRange",
    ),
)

REQUIRED_ADMIN_REQUEST_TRUST_BOUNDARY_TESTS = (
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/AuthenticationPipelineTest.java",
        "executeReplacesForgedSubjectHeaderAfterAuthenticationSucceeds",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/AuthenticationPipelineTest.java",
        "executeClearsForgedSubjectHeaderWhenAuthenticationIsWhitelisted",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/AuthenticationPipelineTest.java",
        "executeClearsForgedSubjectHeaderWhenAuthenticationFails",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/AuthenticationPipelineTest.java",
        "executeClearsForgedSubjectHeaderForCustomAuthenticationContext",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/pipeline/AuthenticationPipelineTest.java",
        "messagingPipelinePreservesWhitelistedAuthenticatedSubject",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/GrpcRequestPipelineFactoryTest.java",
        "createProxyClientAdminContextFactoryDoesNotTrustRawSubjectWhenAuthenticationDisabled",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/HeaderInterceptorTest.java",
        "transportMetadataReplacesClientSuppliedAddressesAndChannelId",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/HeaderInterceptorTest.java",
        "missingTransportMetadataClearsClientSuppliedAddressesAndChannelId",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/HeaderInterceptorTest.java",
        "missingTransportMetadataClearsClientSuppliedProxyProtocolHeaders",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/HeaderInterceptorTest.java",
        "transportProxyProtocolMetadataReplacesOnlyTrustedHeaders",
    ),
    (
        "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/interceptor/HeaderInterceptorTest.java",
        "missingTransportMetadataClearsClientSuppliedBinaryProxyProtocolHeader",
    ),
)

REQUIRED_ENDPOINT_FAILURE_METRICS_WIRING_TESTS = (
    "createDefaultActivityWiresEndpointFailureMetricsRecorder",
)

GITHUB_ARTIFACTS = (
    (
        "RocketMQ PR #10603",
        ["gh", "pr", "view", "10603", "--repo", "apache/rocketmq", "--json", "body", "--jq", ".body"],
    ),
    (
        "rocketmq-apis PR #112",
        ["gh", "pr", "view", "112", "--repo", "apache/rocketmq-apis", "--json", "body", "--jq", ".body"],
    ),
    (
        "RIP-2 issue comment",
        ["gh", "api", "repos/apache/rocketmq/issues/comments/4926996687", "--jq", ".body"],
    ),
)

GITHUB_PR_METADATA = (
    (
        "RocketMQ PR #10603",
        ["gh", "pr", "view", "10603", "--repo", "apache/rocketmq"],
        {
            "state": "OPEN",
            "isDraft": True,
            "headRefName": EXPECTED_BRANCH,
            "baseRefName": "develop",
            "headRepositoryOwner.login": "pilichoumao",
        },
    ),
    (
        "rocketmq-apis PR #112",
        ["gh", "pr", "view", "112", "--repo", "apache/rocketmq-apis"],
        {
            "state": "OPEN",
            "isDraft": True,
            "headRefName": EXPECTED_APIS_BRANCH,
            "baseRefName": "main",
            "headRepositoryOwner.login": "pilichoumao",
        },
    ),
)

GITHUB_PR_FEEDBACK = (
    (
        "RocketMQ PR #10603",
        ["gh", "pr", "view", "10603", "--repo", "apache/rocketmq"],
    ),
    (
        "rocketmq-apis PR #112",
        ["gh", "pr", "view", "112", "--repo", "apache/rocketmq-apis"],
    ),
)

GITHUB_PR_CHECKS = (
    (
        "RocketMQ PR #10603",
        ["gh", "pr", "checks", "10603", "--repo", "apache/rocketmq"],
    ),
    (
        "rocketmq-apis PR #112",
        ["gh", "pr", "checks", "112", "--repo", "apache/rocketmq-apis"],
    ),
)

GITHUB_ISSUE_METADATA = (
    "RIP-2 issue #10599",
    ["gh", "issue", "view", "10599", "--repo", "apache/rocketmq"],
)


def run_command(args, cwd):
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=False)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def read_text(path, errors):
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        errors.append(f"missing required file: {path}")
    except UnicodeDecodeError as exc:
        errors.append(f"cannot read UTF-8 file {path}: {exc}")
    return ""


def check_required_files(root, errors):
    for rel in REQUIRED_FILES:
        path = root / rel
        if not path.is_file():
            errors.append(f"missing required file: {rel}")


def check_reproducible_benchmark_runner(root, errors):
    runner = read_text(root / "dev/run_rip2_benchmark.sh", errors)
    for token in REQUIRED_BENCHMARK_RUNNER_TOKENS:
        if token not in runner:
            errors.append(f"reproducible benchmark runner missing {token}")


def check_maven_proto_version(root, errors):
    pom_text = read_text(root / "pom.xml", errors)
    if PROTO_VERSION_XML not in pom_text:
        errors.append(f"pom.xml missing required rocketmq-proto.version {PROTO_VERSION}")


def check_git_state(root, errors, check_remote, command_runner=run_command):
    code, branch, stderr = command_runner(["git", "branch", "--show-current"], cwd=root)
    if code != 0:
        errors.append(f"cannot determine current branch: {stderr}")
    elif branch != EXPECTED_BRANCH:
        errors.append(f"expected branch {EXPECTED_BRANCH}, got {branch}")

    code, status, stderr = command_runner(["git", "status", "--short", "--untracked-files=all"], cwd=root)
    if code != 0:
        errors.append(f"cannot determine git status: {stderr}")
    elif status:
        errors.append(f"working tree is not clean:\n{status}")

    if not check_remote:
        return

    code, head, stderr = command_runner(["git", "rev-parse", "HEAD"], cwd=root)
    if code != 0:
        errors.append(f"cannot determine HEAD: {stderr}")
        return
    code, remote, stderr = command_runner(
        ["git", "ls-remote", "origin", f"refs/heads/{EXPECTED_BRANCH}"],
        cwd=root,
    )
    if code != 0:
        errors.append(f"cannot query origin/{EXPECTED_BRANCH}: {stderr}")
        return
    remote_head = remote.split()[0] if remote else ""
    if remote_head != head:
        errors.append(f"origin/{EXPECTED_BRANCH} is {remote_head}, local HEAD is {head}")


def resolve_apis_remote(apis_root, errors, apis_remote, command_runner=run_command):
    if apis_remote != "auto":
        return apis_remote

    code, upstream, stderr = command_runner(
        ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
        cwd=apis_root,
    )
    if code != 0:
        errors.append(
            "cannot determine rocketmq-apis upstream remote for auto check: "
            f"{stderr}. Use --apis-remote <remote> to override."
        )
        return ""
    if "/" not in upstream:
        errors.append(f"rocketmq-apis upstream remote is malformed: {upstream}")
        return ""
    remote_name, remote_branch = upstream.split("/", 1)
    if remote_branch != EXPECTED_APIS_BRANCH:
        errors.append(
            f"expected rocketmq-apis upstream branch {EXPECTED_APIS_BRANCH}, got {upstream}"
        )
        return ""
    return remote_name


def check_apis_git_state(apis_root, errors, check_remote, apis_remote, command_runner=run_command):
    code, branch, stderr = command_runner(["git", "branch", "--show-current"], cwd=apis_root)
    if code != 0:
        errors.append(f"cannot determine rocketmq-apis branch: {stderr}")
    elif branch != EXPECTED_APIS_BRANCH:
        errors.append(f"expected rocketmq-apis branch {EXPECTED_APIS_BRANCH}, got {branch}")

    code, status, stderr = command_runner(["git", "status", "--short", "--untracked-files=all"], cwd=apis_root)
    if code != 0:
        errors.append(f"cannot determine rocketmq-apis git status: {stderr}")
    elif status:
        errors.append(f"rocketmq-apis working tree is not clean:\n{status}")

    if not check_remote:
        return

    code, head, stderr = command_runner(["git", "rev-parse", "HEAD"], cwd=apis_root)
    if code != 0:
        errors.append(f"cannot determine rocketmq-apis HEAD: {stderr}")
        return
    remote_name = resolve_apis_remote(apis_root, errors, apis_remote, command_runner=command_runner)
    if not remote_name:
        return
    code, remote, stderr = command_runner(
        ["git", "ls-remote", remote_name, f"refs/heads/{EXPECTED_APIS_BRANCH}"],
        cwd=apis_root,
    )
    if code != 0:
        errors.append(f"cannot query {remote_name}/{EXPECTED_APIS_BRANCH}: {stderr}")
        return
    remote_head = remote.split()[0] if remote else ""
    if remote_head != head:
        errors.append(f"{remote_name}/{EXPECTED_APIS_BRANCH} is {remote_head}, local HEAD is {head}")


def check_proto(root, apis_root, errors):
    doc_proto_path = root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto"
    api_proto_path = apis_root / "apache/rocketmq/v2/admin.proto"
    doc_proto = read_text(doc_proto_path, errors)
    api_proto = read_text(api_proto_path, errors)
    if doc_proto and api_proto and doc_proto != api_proto:
        errors.append("public API draft proto mirror differs from ../rocketmq-apis/apache/rocketmq/v2/admin.proto")
    for token in REQUIRED_PROTO_MESSAGES:
        if token not in doc_proto:
            errors.append(f"public API draft proto missing {token}")
    normalized_proto = " ".join(
        doc_proto
        .replace("(", " ( ")
        .replace(")", " ) ")
        .split()
    )
    for signature in REQUIRED_PROTO_RPC_SIGNATURES:
        if signature not in normalized_proto:
            errors.append(f"public API draft proto missing signature {signature}")
    for value in REQUIRED_PROXY_SCOPE_VALUES:
        if value not in normalized_proto:
            errors.append(f"public API draft proto missing enum value {value}")
    for field in REQUIRED_PROTO_FIELDS:
        if field not in normalized_proto:
            errors.append(f"public API draft proto missing field {field}")


def check_apis_java_build_metadata(apis_root, errors):
    version_text = read_text(apis_root / "java/VERSION", errors).strip()
    if version_text != APIS_JAVA_VERSION:
        errors.append(f"rocketmq-apis java/VERSION expected {APIS_JAVA_VERSION}, got {version_text}")
    build_text = read_text(apis_root / "java/BUILD.bazel", errors)
    for token in (
        'name = "rocketmq-proto"',
        "maven_coordinates=org.apache.rocketmq:rocketmq-proto:{pom_version}",
        "assemble_maven(",
        'target = ":rocketmq-proto"',
        'version_file = ":VERSION"',
        "deploy_maven(",
        'target = ":assemble-maven"',
    ):
        if token not in build_text:
            errors.append(f"rocketmq-apis java/BUILD.bazel missing {token}")


def check_generated_artifact(m2_repository, errors):
    artifact_dir = (
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto"
        / PROTO_VERSION
    )
    jar_path = artifact_dir / f"rocketmq-proto-{PROTO_VERSION}.jar"
    if not jar_path.is_file():
        errors.append(f"missing local generated rocketmq-proto artifact: {jar_path}")
        return None
    pom_path = artifact_dir / f"rocketmq-proto-{PROTO_VERSION}.pom"
    metadata_path = artifact_dir / "maven-metadata-local.xml"
    repositories_path = artifact_dir / "_remote.repositories"
    check_generated_artifact_text_file(
        pom_path,
        "pom",
        (
            "<groupId>org.apache.rocketmq</groupId>",
            "<artifactId>rocketmq-proto</artifactId>",
            f"<version>{PROTO_VERSION}</version>",
        ),
        errors,
    )
    check_generated_artifact_text_file(
        metadata_path,
        "maven metadata",
        (
            "<groupId>org.apache.rocketmq</groupId>",
            "<artifactId>rocketmq-proto</artifactId>",
            f"<version>{PROTO_VERSION}</version>",
            "<localCopy>true</localCopy>",
            "<extension>jar</extension>",
            "<extension>pom</extension>",
        ),
        errors,
    )
    check_generated_artifact_text_file(
        repositories_path,
        "repository marker",
        (
            f"rocketmq-proto-{PROTO_VERSION}.jar>=",
            f"rocketmq-proto-{PROTO_VERSION}.pom>=",
        ),
        errors,
    )
    try:
        with zipfile.ZipFile(jar_path) as jar_file:
            entries = set(jar_file.namelist())
    except zipfile.BadZipFile as exc:
        errors.append(f"generated rocketmq-proto artifact is not a valid jar: {exc}")
        return None
    for entry in REQUIRED_JAR_ENTRIES:
        if entry not in entries:
            errors.append(f"generated rocketmq-proto artifact missing {entry}")
    return file_sha256(jar_path)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_generated_artifact_text_file(path, label, required_tokens, errors):
    if not path.is_file():
        errors.append(f"generated rocketmq-proto artifact missing {label}: {path}")
        return
    text = read_text(path, errors)
    for token in required_tokens:
        if token not in text:
            errors.append(f"generated rocketmq-proto artifact {label} missing {token}")


def check_submission_evidence(root, errors, artifact_sha256=None):
    combined_docs = "\n".join(read_text(root / rel, errors) for rel in REQUIRED_DOCS)
    required_tokens = (
        FOCUSED_RESULT,
        FOCUSED_FINISHED_AT,
        BROAD_RESULT,
        BROAD_FINISHED_AT,
        PACKAGE_SMOKE_FINISHED_AT,
        "3.576 ms",
        "0.681 ms",
        "Dashboard CLIENT-01",
        "external",
        INTERNAL_ERROR_PUBLIC_ENDPOINT_TEST,
        *REQUIRED_EXTERNAL_GATE_TOKENS,
    )
    for token in required_tokens:
        if token not in combined_docs:
            if token == PACKAGE_SMOKE_FINISHED_AT:
                errors.append(f"package smoke evidence missing {token}")
            elif token == INTERNAL_ERROR_PUBLIC_ENDPOINT_TEST:
                errors.append(f"submission evidence missing INTERNAL_SERVER_ERROR public endpoint evidence {token}")
            else:
                errors.append(f"submission evidence missing {token}")
    for rel in CONSTRAINED_HEAP_EVIDENCE_DOCS:
        document = read_text(root / rel, errors)
        for token in REQUIRED_CONSTRAINED_HEAP_EVIDENCE:
            if token not in document:
                errors.append(f"constrained heap benchmark evidence missing {token} in {rel}")
    if artifact_sha256:
        artifact_sha_token = f"rocketmq-proto jar SHA-256: {artifact_sha256}"
        for rel in ARTIFACT_SHA_EVIDENCE_DOCS:
            if artifact_sha_token not in read_text(root / rel, errors):
                errors.append(f"generated artifact SHA-256 evidence missing in {rel}")
    for token in REQUIRED_PUBLIC_SCOPE_GATE_TOKENS:
        if token not in combined_docs:
            errors.append(f"submission evidence missing M1 public scope gate {token}")
    for link in PUBLIC_REVIEW_ARTIFACT_LINKS:
        if link not in combined_docs:
            errors.append(f"submission evidence missing public review artifact link {link}")
    stale_tokens = (
        OLD_FOCUSED_RESULT,
        OLD_BROAD_RESULT,
        PREVIOUS_FOCUSED_RESULT,
        PREVIOUS_BROAD_RESULT,
        OLD_FOCUSED_FINISHED_AT,
        OLD_BROAD_FINISHED_AT,
        OLD_PACKAGE_SMOKE_FINISHED_AT,
        PREVIOUS_FOCUSED_FINISHED_AT,
        PREVIOUS_BROAD_FINISHED_AT,
        PREVIOUS_PACKAGE_SMOKE_FINISHED_AT,
        RECENT_FOCUSED_FINISHED_AT,
        RECENT_BROAD_FINISHED_AT,
        RECENT_PACKAGE_SMOKE_FINISHED_AT,
        REFRESHED_FOCUSED_FINISHED_AT,
        REFRESHED_BROAD_FINISHED_AT,
        REFRESHED_PACKAGE_SMOKE_FINISHED_AT,
        PRE_ERROR_BODY_FOCUSED_FINISHED_AT,
        PRE_ERROR_BODY_BROAD_FINISHED_AT,
        PRE_ERROR_BODY_PACKAGE_SMOKE_FINISHED_AT,
        PRE_COVERAGE_REFRESH_FOCUSED_FINISHED_AT,
        PRE_COVERAGE_REFRESH_BROAD_FINISHED_AT,
        PRE_COVERAGE_REFRESH_PACKAGE_SMOKE_FINISHED_AT,
        PRE_ENDPOINT_METRICS_FOCUSED_RESULT,
        PRE_ENDPOINT_METRICS_BROAD_RESULT,
        PRE_ENDPOINT_METRICS_FOCUSED_FINISHED_AT,
        PRE_ENDPOINT_METRICS_BROAD_FINISHED_AT,
        PRE_ENDPOINT_METRICS_PACKAGE_SMOKE_FINISHED_AT,
        PRE_REQUEST_HARDENING_BROAD_RESULT,
        PRE_REQUEST_HARDENING_FOCUSED_FINISHED_AT,
        PRE_REQUEST_HARDENING_BROAD_FINISHED_AT,
        PRE_REQUEST_HARDENING_PACKAGE_SMOKE_FINISHED_AT,
        PRE_TRUST_BOUNDARY_BROAD_RESULT,
        PRE_TRUST_BOUNDARY_FOCUSED_FINISHED_AT,
        PRE_TRUST_BOUNDARY_BROAD_FINISHED_AT,
        PRE_TRUST_BOUNDARY_PACKAGE_SMOKE_FINISHED_AT,
        PRE_BINARY_HEADER_BROAD_RESULT,
        PRE_BINARY_HEADER_BROAD_FINISHED_AT,
        PRE_BINARY_HEADER_PACKAGE_SMOKE_FINISHED_AT,
        PRE_PRODUCTION_INTERCEPTOR_BROAD_RESULT,
        PRE_PRODUCTION_INTERCEPTOR_BROAD_FINISHED_AT,
        PRE_PRODUCTION_INTERCEPTOR_PACKAGE_SMOKE_FINISHED_AT,
        PRE_CONSTRAINED_HEAP_BROAD_RESULT,
        PRE_CONSTRAINED_HEAP_BROAD_FINISHED_AT,
        PRE_CONSTRAINED_HEAP_PACKAGE_SMOKE_FINISHED_AT,
        PRE_TEARDOWN_HARDENING_BROAD_RESULT,
        PRE_TEARDOWN_HARDENING_BROAD_FINISHED_AT,
        PRE_TEARDOWN_HARDENING_PACKAGE_SMOKE_FINISHED_AT,
        PRE_ADMIN_EXPOSURE_BROAD_RESULT,
        PRE_ADMIN_EXPOSURE_BROAD_FINISHED_AT,
        PRE_ADMIN_EXPOSURE_PACKAGE_SMOKE_FINISHED_AT,
    )
    for token in stale_tokens:
        if token in combined_docs:
            if (
                "04:07:14" in token
                or "06:27:48" in token
                or "07:54:35" in token
                or "08:31:23" in token
                or "08:47:54" in token
                or "09:02:11" in token
                or "09:51:17" in token
                or "10:14:37" in token
                or "11:57:46" in token
                or "18:48:25" in token
                or "23:09:28" in token
            ):
                errors.append(f"stale package smoke evidence remains: {token}")
            elif (
                "728" in token
                or "730" in token
                or "731" in token
                or "735" in token
                or "05:14:32" in token
                or "06:06:44" in token
                or "07:53:37" in token
                or "08:28:12" in token
                or "08:46:56" in token
                or "09:01:07" in token
                or "09:50:12" in token
                or "10:13:01" in token
                or "741" in token
                or "11:56:38" in token
                or "748" in token
                or "18:46:34" in token
                or "760" in token
                or "23:07:13" in token
            ):
                errors.append(f"stale broad verification evidence remains: {token}")
            else:
                errors.append(f"stale focused verification evidence remains: {token}")


def check_required_markdown_fences(root, errors):
    for rel in REQUIRED_DOCS:
        text = read_text(root / rel, errors)
        if text.count("```") % 2 != 0:
            errors.append(f"unbalanced markdown code fences in {rel}")


def check_manual_smoke_contract(root, errors):
    for rel in FINAL_SMOKE_DOCS:
        text = read_text(root / rel, errors)
        for token in REQUIRED_MANUAL_SMOKE_TOKENS:
            if token not in text:
                errors.append(f"manual smoke contract missing {token} in {rel}")


def check_review_runbook_contract(root, errors):
    for rel in REVIEW_RUNBOOK_DOCS:
        text = read_text(root / rel, errors)
        for token in REQUIRED_REVIEW_RUNBOOK_TOKENS:
            if token not in text:
                errors.append(f"review runbook must pin commits; missing {token} in {rel}")
        for token in FORBIDDEN_REVIEW_RUNBOOK_TOKENS:
            if token in text:
                errors.append(f"review runbook must pin commits; moving branch command remains in {rel}")


def check_plan_checkboxes(root, errors):
    plan_text = read_text(root / PLAN_FILE, errors)
    for line_number, line in enumerate(plan_text.splitlines(), start=1):
        if line.lstrip().startswith("- [ ]"):
            errors.append(f"unfinished plan checkbox in {PLAN_FILE}:{line_number}")


def check_stale_checkpoint_references(root, errors):
    checked_files = REQUIRED_DOCS + (PLAN_FILE,)
    for rel in checked_files:
        text = read_text(root / rel, errors)
        for checkpoint in STALE_IMPLEMENTATION_CHECKPOINTS:
            if checkpoint in text:
                errors.append(f"stale implementation checkpoint {checkpoint} remains in {rel}")


def check_source_wiring(root, errors):
    app_text = read_text(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java",
        errors,
    )
    for token in (
        "ProxyAdminServiceGrpc.ProxyAdminServiceImplBase",
        "listClients",
        "describeClient",
        "listClientsByGroup",
        "listClientsByTopic",
    ):
        if token not in app_text:
            errors.append(f"GrpcProxyAdminApplication missing {token}")
    startup_text = read_text(root / "proxy/src/main/java/org/apache/rocketmq/proxy/ProxyStartup.java", errors)
    for token in (
        "GrpcProxyAdminApplication",
        "createProxyAdminGrpcBindableServices",
        "configureProxyAdminGrpcServer",
        "isEnableProxyAdminGrpcServer()",
        "getProxyAdminGrpcServerPort()",
        "new ThreadPoolExecutor.AbortPolicy()",
    ):
        if token not in startup_text:
            errors.append(f"ProxyStartup missing {token}")
    admin_listener_marker = "static GrpcServerBuilder configureProxyAdminGrpcServer"
    if admin_listener_marker in startup_text:
        admin_listener_text = startup_text.split(admin_listener_marker, 1)[1]
        for token in (
            "ChannelzService.newInstance",
            "ProtoReflectionService.newInstance",
            "getProxyClientAdminPeerGrpcService",
        ):
            if token in admin_listener_text:
                errors.append(f"public admin listener exposes internal service: {token}")
    config_text = read_text(root / "proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java", errors)
    for token in (
        "private boolean enableProxyAdminGrpcServer = false",
        "private Integer proxyAdminGrpcServerPort = 8082",
    ):
        if token not in config_text:
            errors.append(f"ProxyConfig missing {token}")
    pipeline_factory_text = read_text(
        root / "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/GrpcRequestPipelineFactory.java",
        errors,
    )
    if "AuthenticationPipeline.forProxyAdmin" not in pipeline_factory_text:
        errors.append("GrpcRequestPipelineFactory missing strict proxy admin authentication pipeline")


def check_required_test_coverage(root, errors):
    app_test_text = read_text(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
        errors,
    )
    for test_name in REQUIRED_GENERATED_PUBLIC_ENDPOINT_TESTS:
        if test_name not in app_test_text:
            coverage_label = "generated public endpoint coverage"
            if "InternalServerError" in test_name:
                coverage_label = "generated public endpoint INTERNAL_SERVER_ERROR coverage"
            errors.append(
                f"GrpcProxyAdminApplicationTest missing {coverage_label}: "
                f"{test_name}"
            )

    startup_test_text = read_text(root / "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java", errors)
    for test_name in REQUIRED_ADMIN_SERVER_ISOLATION_TESTS:
        if test_name not in startup_test_text:
            errors.append(
                "ProxyStartupTest missing admin server isolation coverage: "
                f"{test_name}"
            )

    production_e2e_text = read_text(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "GrpcProxyAdminProductionInterceptorE2ETest.java",
        errors,
    )
    for test_name in REQUIRED_PRODUCTION_INTERCEPTOR_E2E_TESTS:
        if test_name not in production_e2e_text:
            errors.append(
                "proxy admin production interceptor dual server E2E missing: "
                f"{test_name}"
            )

    endpoint_executor_test_text = read_text(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "ProxyClientAdminEndpointExecutorTest.java",
        errors,
    )
    for test_name in REQUIRED_ENDPOINT_FAILURE_METRICS_TESTS:
        if test_name not in endpoint_executor_test_text:
            errors.append(
                "ProxyClientAdminEndpointExecutorTest missing endpoint failure metrics coverage: "
                f"{test_name}"
            )
    for test_name in REQUIRED_ENDPOINT_CONTEXT_PROPAGATION_TESTS:
        if test_name not in endpoint_executor_test_text:
            errors.append(
                "ProxyClientAdminEndpointExecutorTest missing endpoint context propagation coverage: "
                f"{test_name}"
            )

    for path, test_name in REQUIRED_BOUNDED_SHUTDOWN_TESTS:
        test_text = read_text(root / path, errors)
        if test_name not in test_text:
            errors.append(
                "Proxy admin test suite missing bounded shutdown coverage: "
                f"{test_name}"
            )

    for path, test_name in REQUIRED_WIDE_CONNECT_TIME_TESTS:
        test_text = read_text(root / path, errors)
        if test_name not in test_text:
            errors.append(
                "Proxy admin test suite missing wide connect-time coverage: "
                f"{test_name}"
            )

    for path, test_name in REQUIRED_ADMIN_REQUEST_TRUST_BOUNDARY_TESTS:
        test_text = read_text(root / path, errors)
        if test_name not in test_text:
            errors.append(
                "Proxy admin test suite missing admin request trust boundary coverage: "
                f"{test_name}"
            )

    wiring_test_text = read_text(
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/"
        "GrpcProxyAdminWiringTest.java",
        errors,
    )
    for test_name in REQUIRED_ENDPOINT_FAILURE_METRICS_WIRING_TESTS:
        if test_name not in wiring_test_text:
            errors.append(
                "GrpcProxyAdminWiringTest missing endpoint failure metrics coverage: "
                f"{test_name}"
            )


def check_github_artifacts(root, apis_root, errors, require_github_checks=False,
    command_runner=run_command):
    code, head, stderr = command_runner(["git", "rev-parse", "HEAD"], cwd=root)
    if code != 0:
        errors.append(f"cannot determine HEAD for GitHub artifact checks: {stderr}")
        return
    code, apis_head, stderr = command_runner(["git", "rev-parse", "HEAD"], cwd=apis_root)
    if code != 0:
        errors.append(f"cannot determine rocketmq-apis HEAD for GitHub artifact checks: {stderr}")
        return
    for label, args in GITHUB_ARTIFACTS:
        code, body, stderr = command_runner(args, cwd=root)
        if code != 0:
            errors.append(f"cannot read {label}: {stderr}")
            continue
        for checkpoint in STALE_IMPLEMENTATION_CHECKPOINTS:
            if checkpoint in body:
                errors.append(f"{label} contains stale implementation checkpoint {checkpoint}")
        for token in GITHUB_EXTERNAL_GATE_TOKENS:
            if token not in body:
                errors.append(f"{label} missing external gate evidence {token}")
        for token in GITHUB_COVERAGE_EVIDENCE:
            if token not in body:
                errors.append(f"{label} missing current coverage evidence {token}")
        for token in STALE_GITHUB_COVERAGE_EVIDENCE:
            if token in body:
                errors.append(f"{label} contains stale coverage evidence {token}")
        for token in REQUIRED_PUBLIC_SCOPE_GATE_TOKENS:
            if token not in body:
                errors.append(f"{label} missing M1 public scope gate evidence {token}")
        if head not in body:
            errors.append(f"{label} does not reference current HEAD {head}")
        if apis_head not in body:
            errors.append(f"{label} does not reference rocketmq-apis HEAD {apis_head}")
        if FULL_GUARD_COMMAND not in body:
            errors.append(f"{label} does not include full submission guard command")
        if "RIP-2 submission guard passed." not in body:
            errors.append(f"{label} does not include submission guard evidence")
    for label, base_args, expected_metadata in GITHUB_PR_METADATA:
        args = base_args + [
            "--json",
            "state,isDraft,headRefName,baseRefName,headRepositoryOwner",
            "--jq",
            ".",
        ]
        code, body, stderr = command_runner(args, cwd=root)
        if code != 0:
            errors.append(f"cannot read {label} metadata: {stderr}")
            continue
        try:
            metadata = json.loads(body)
        except json.JSONDecodeError as exc:
            errors.append(f"cannot parse {label} metadata: {exc}")
            continue
        for key, expected_value in expected_metadata.items():
            actual_value = nested_value(metadata, key)
            if actual_value != expected_value:
                errors.append(
                    f"{label} metadata expected {key}={expected_value!r}, got {actual_value!r}"
                )
    for label, base_args in GITHUB_PR_FEEDBACK:
        args = base_args + ["--json", "comments,reviews", "--jq", "."]
        code, body, stderr = command_runner(args, cwd=root)
        if code != 0:
            errors.append(f"cannot read {label} feedback: {stderr}")
            continue
        try:
            feedback = json.loads(body)
        except json.JSONDecodeError as exc:
            errors.append(f"cannot parse {label} feedback: {exc}")
            continue
        comment_count = len(feedback.get("comments") or [])
        review_count = len(feedback.get("reviews") or [])
        if comment_count or review_count:
            errors.append(
                f"{label} has unreviewed feedback: {comment_count} comments, {review_count} reviews"
            )
    check_github_pr_checks(
        root,
        errors,
        require_github_checks=require_github_checks,
        command_runner=command_runner,
    )
    check_github_issue_metadata(root, errors, command_runner=command_runner)


def check_github_pr_checks(root, errors, require_github_checks=False, command_runner=run_command):
    for label, base_args in GITHUB_PR_CHECKS:
        args = base_args + ["--json", "name,state,bucket,link"]
        code, stdout, stderr = command_runner(args, cwd=root)
        output = stdout or stderr
        if code != 0 and "no checks reported" in output:
            if require_github_checks:
                errors.append(f"{label} has no reported checks")
            continue
        if code != 0 and not stdout:
            errors.append(f"cannot read {label} checks: {stderr}")
            continue
        try:
            checks = json.loads(stdout)
        except json.JSONDecodeError as exc:
            errors.append(f"cannot parse {label} checks: {exc}")
            continue
        if require_github_checks and not checks:
            errors.append(f"{label} has no reported checks")
            continue
        for check in checks:
            bucket = check.get("bucket")
            if bucket not in ("pass", "skipping"):
                errors.append(
                    f"{label} has non-passing check {check.get('name')!r}: "
                    f"bucket={bucket!r}, state={check.get('state')!r}, link={check.get('link')!r}"
                )


def check_github_issue_metadata(root, errors, command_runner=run_command):
    label, base_args = GITHUB_ISSUE_METADATA
    args = base_args + ["--json", "state,title,comments", "--jq", "."]
    code, body, stderr = command_runner(args, cwd=root)
    if code != 0:
        errors.append(f"cannot read {label}: {stderr}")
        return
    try:
        issue = json.loads(body)
    except json.JSONDecodeError as exc:
        errors.append(f"cannot parse {label}: {exc}")
        return
    if issue.get("state") != "OPEN":
        errors.append(f"{label} expected state OPEN, got {issue.get('state')!r}")
    title = issue.get("title") or ""
    if "[RIP-2]" not in title or "Proxy Admin" not in title:
        errors.append(f"{label} title no longer matches RIP-2 Proxy Admin: {title!r}")
    comments = issue.get("comments") or []
    if not comments:
        errors.append(f"{label} has no comments; expected submission summary comment")
        return
    last_comment_url = comments[-1].get("url") if isinstance(comments[-1], dict) else None
    if last_comment_url != RIP2_ISSUE_COMMENT_URL:
        errors.append(
            f"RIP-2 issue has comments after submission summary: latest comment is {last_comment_url!r}"
        )


def nested_value(data, key):
    value = data
    for part in key.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def run_checks(
    root,
    apis_root,
    m2_repository,
    check_git=True,
    check_remote=False,
    check_apis_remote=False,
    apis_remote=DEFAULT_APIS_REMOTE,
    check_github=False,
    require_github_checks=False,
    command_runner=run_command,
):
    root = Path(root).resolve()
    apis_root = Path(apis_root).resolve()
    m2_repository = Path(m2_repository).expanduser().resolve()
    errors = []
    check_required_files(root, errors)
    check_reproducible_benchmark_runner(root, errors)
    check_maven_proto_version(root, errors)
    if check_git:
        check_git_state(root, errors, check_remote, command_runner=command_runner)
    if check_apis_remote:
        check_apis_git_state(
            apis_root,
            errors,
            check_remote=True,
            apis_remote=apis_remote,
            command_runner=command_runner,
        )
    check_proto(root, apis_root, errors)
    check_apis_java_build_metadata(apis_root, errors)
    artifact_sha256 = check_generated_artifact(m2_repository, errors)
    check_submission_evidence(root, errors, artifact_sha256)
    check_required_markdown_fences(root, errors)
    check_manual_smoke_contract(root, errors)
    check_review_runbook_contract(root, errors)
    check_plan_checkboxes(root, errors)
    check_stale_checkpoint_references(root, errors)
    check_source_wiring(root, errors)
    check_required_test_coverage(root, errors)
    if check_github:
        check_github_artifacts(
            root,
            apis_root,
            errors,
            require_github_checks=require_github_checks,
            command_runner=command_runner,
        )
    return errors


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Check RIP-2 Proxy Admin submission evidence.")
    parser.add_argument("--root", default=".", help="RocketMQ repository root. Defaults to current directory.")
    parser.add_argument(
        "--apis-root",
        default="../rocketmq-apis",
        help="Sibling rocketmq-apis repository root. Defaults to ../rocketmq-apis.",
    )
    parser.add_argument(
        "--m2-repository",
        default=str(Path.home() / ".m2/repository"),
        help="Maven local repository path.",
    )
    parser.add_argument("--skip-git", action="store_true", help="Skip branch and worktree checks.")
    parser.add_argument("--check-remote", action="store_true", help="Check origin/rip2-proxy-admin-m1 equals HEAD.")
    parser.add_argument(
        "--check-apis-remote",
        action="store_true",
        help="Check the sibling rocketmq-apis proposal branch equals its configured remote.",
    )
    parser.add_argument(
        "--apis-remote",
        default=DEFAULT_APIS_REMOTE,
        help=(
            "rocketmq-apis remote name used with --check-apis-remote. Defaults to auto, "
            "which uses the branch upstream remote."
        ),
    )
    parser.add_argument("--check-github", action="store_true", help="Check public PR and issue text references HEAD.")
    parser.add_argument(
        "--require-github-checks",
        action="store_true",
        help="Require both public PRs to report CI checks; implies --check-github.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    errors = run_checks(
        root=args.root,
        apis_root=args.apis_root,
        m2_repository=args.m2_repository,
        check_git=not args.skip_git,
        check_remote=args.check_remote,
        check_apis_remote=args.check_apis_remote,
        apis_remote=args.apis_remote,
        check_github=args.check_github or args.require_github_checks,
        require_github_checks=args.require_github_checks,
    )
    if errors:
        print("RIP-2 submission guard failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("RIP-2 submission guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
