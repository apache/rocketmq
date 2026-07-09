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
  rpc ListClientsByGroup(ListClientsByGroupRequest) returns (ListClientsResponse) {}
  rpc ListClientsByTopic(ListClientsByTopicRequest) returns (ListClientsResponse) {}
}

message ListClientsRequest {}
message DescribeClientRequest {}
message DescribeClientResponse {}
message ListClientsByGroupRequest {}
message ListClientsByTopicRequest {}
message ListClientsResponse {}
message ProxyClient {}
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
        jar_file.writestr("apache/rocketmq/v2/ProxyAdminServiceGrpc.class", b"")
        jar_file.writestr("apache/rocketmq/v2/ListClientsRequest.class", b"")
        jar_file.writestr("apache/rocketmq/v2/ProxyClient.class", b"")


def create_submission_tree(root, apis_root, m2_repository):
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
        root / "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
        "class GrpcProxyAdminApplicationTest {}\n",
    )
    write(root / "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java", "class ProxyStartupTest {}\n")
    write(root / "docs/en/rip2-proxy-admin-m1-public-api-draft.proto", PROTO)
    write(apis_root / "apache/rocketmq/v2/admin.proto", PROTO)

    submission = """
Tests run: 54, Failures: 0, Errors: 0, Skipped: 0
Finished at: 2026-07-10T05:58:49+08:00
Tests run: 730, Failures: 0, Errors: 0, Skipped: 0
Finished at: 2026-07-10T06:06:44+08:00
3.576 ms
0.681 ms
Dashboard CLIENT-01
external validation item
"""
    for rel in rip2_submission_guard.REQUIRED_DOCS:
        write(root / rel, submission)
    create_snapshot_jar(m2_repository)


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
            package_path = root / "docs/en/rip2-proxy-admin-m1-submission-package.md"
            package_path.write_text(
                package_path.read_text(encoding="utf-8").replace("Tests run: 730", "Tests run: 728"),
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


if __name__ == "__main__":
    unittest.main()
