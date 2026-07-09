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

message ListClientsRequest {}
message ListClientsResponse {}
message DescribeClientRequest {}
message DescribeClientResponse {}
message ListClientsByGroupRequest {}
message ListClientsByGroupResponse {}
message ListClientsByTopicRequest {}
message ListClientsByTopicResponse {}
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
        for entry in rip2_submission_guard.REQUIRED_JAR_ENTRIES:
            jar_file.writestr(entry, b"")


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
Finished at: 2026-07-10T06:27:48+08:00
3.576 ms
0.681 ms
Dashboard CLIENT-01
external validation item
"""
    for rel in rip2_submission_guard.REQUIRED_DOCS:
        write(root / rel, submission)
    create_snapshot_jar(m2_repository)


def fake_github_runner(expected_head, rocketmq_body, api_body, issue_body, apis_head=None):
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
                        "Finished at: 2026-07-10T06:27:48+08:00\n",
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

    def test_guard_reports_proto_missing_public_rpc_response_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = base / "rocketmq"
            apis_root = base / "rocketmq-apis"
            m2_repository = base / "m2"
            create_submission_tree(root, apis_root, m2_repository)
            incomplete_proto = PROTO.replace("message ListClientsByGroupResponse {}\n", "")
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


if __name__ == "__main__":
    unittest.main()
