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
import subprocess
import sys
import zipfile
from pathlib import Path


EXPECTED_BRANCH = "rip2-proxy-admin-m1"
EXPECTED_APIS_BRANCH = "rip2-proxy-admin-public-api"
DEFAULT_APIS_REMOTE = "auto"
PROTO_VERSION = "2.2.0-rip2-SNAPSHOT"
PROTO_VERSION_XML = f"<rocketmq-proto.version>{PROTO_VERSION}</rocketmq-proto.version>"
FOCUSED_RESULT = "Tests run: 54, Failures: 0, Errors: 0, Skipped: 0"
FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T05:58:49+08:00"
BROAD_RESULT = "Tests run: 730, Failures: 0, Errors: 0, Skipped: 0"
BROAD_FINISHED_AT = "Finished at: 2026-07-10T06:06:44+08:00"
PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T06:27:48+08:00"
OLD_FOCUSED_RESULT = "Tests run: 52, Failures: 0, Errors: 0, Skipped: 0"
OLD_BROAD_RESULT = "Tests run: 728, Failures: 0, Errors: 0, Skipped: 0"
OLD_FOCUSED_FINISHED_AT = "Finished at: 2026-07-10T04:04:54+08:00"
OLD_BROAD_FINISHED_AT = "Finished at: 2026-07-10T05:14:32+08:00"
OLD_PACKAGE_SMOKE_FINISHED_AT = "Finished at: 2026-07-10T04:07:14+08:00"

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

REQUIRED_FILES = (
    "pom.xml",
    "proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplication.java",
    "proxy/src/test/java/org/apache/rocketmq/proxy/grpc/v2/admin/GrpcProxyAdminApplicationTest.java",
    "proxy/src/test/java/org/apache/rocketmq/proxy/ProxyStartupTest.java",
    "docs/en/rip2-proxy-admin-m1-public-api-draft.proto",
) + REQUIRED_DOCS

REQUIRED_PROTO_MESSAGES = (
    "service ProxyAdminService",
    "rpc ListClients",
    "rpc DescribeClient",
    "rpc ListClientsByGroup",
    "rpc ListClientsByTopic",
    "message ProxyClient",
)

REQUIRED_JAR_ENTRIES = (
    "apache/rocketmq/v2/ProxyAdminServiceGrpc.class",
    "apache/rocketmq/v2/ListClientsRequest.class",
    "apache/rocketmq/v2/ProxyClient.class",
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


def check_generated_artifact(m2_repository, errors):
    jar_path = (
        m2_repository
        / "org/apache/rocketmq/rocketmq-proto"
        / PROTO_VERSION
        / f"rocketmq-proto-{PROTO_VERSION}.jar"
    )
    if not jar_path.is_file():
        errors.append(f"missing local generated rocketmq-proto artifact: {jar_path}")
        return
    try:
        with zipfile.ZipFile(jar_path) as jar_file:
            entries = set(jar_file.namelist())
    except zipfile.BadZipFile as exc:
        errors.append(f"generated rocketmq-proto artifact is not a valid jar: {exc}")
        return
    for entry in REQUIRED_JAR_ENTRIES:
        if entry not in entries:
            errors.append(f"generated rocketmq-proto artifact missing {entry}")


def check_submission_evidence(root, errors):
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
    )
    for token in required_tokens:
        if token not in combined_docs:
            if token == PACKAGE_SMOKE_FINISHED_AT:
                errors.append(f"package smoke evidence missing {token}")
            else:
                errors.append(f"submission evidence missing {token}")
    stale_tokens = (
        OLD_FOCUSED_RESULT,
        OLD_BROAD_RESULT,
        OLD_FOCUSED_FINISHED_AT,
        OLD_BROAD_FINISHED_AT,
        OLD_PACKAGE_SMOKE_FINISHED_AT,
    )
    for token in stale_tokens:
        if token in combined_docs:
            if "04:07:14" in token:
                errors.append(f"stale package smoke evidence remains: {token}")
            elif "728" in token or "05:14:32" in token:
                errors.append(f"stale broad verification evidence remains: {token}")
            else:
                errors.append(f"stale focused verification evidence remains: {token}")


def check_required_markdown_fences(root, errors):
    for rel in REQUIRED_DOCS:
        text = read_text(root / rel, errors)
        if text.count("```") % 2 != 0:
            errors.append(f"unbalanced markdown code fences in {rel}")


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


def check_github_artifacts(root, apis_root, errors, command_runner=run_command):
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
        if head not in body:
            errors.append(f"{label} does not reference current HEAD {head}")
        if apis_head not in body:
            errors.append(f"{label} does not reference rocketmq-apis HEAD {apis_head}")
        if "RIP-2 submission guard passed." not in body:
            errors.append(f"{label} does not include submission guard evidence")


def run_checks(
    root,
    apis_root,
    m2_repository,
    check_git=True,
    check_remote=False,
    check_apis_remote=False,
    apis_remote=DEFAULT_APIS_REMOTE,
    check_github=False,
    command_runner=run_command,
):
    root = Path(root).resolve()
    apis_root = Path(apis_root).resolve()
    m2_repository = Path(m2_repository).expanduser().resolve()
    errors = []
    check_required_files(root, errors)
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
    check_generated_artifact(m2_repository, errors)
    check_submission_evidence(root, errors)
    check_required_markdown_fences(root, errors)
    check_source_wiring(root, errors)
    if check_github:
        check_github_artifacts(root, apis_root, errors, command_runner=command_runner)
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
        check_github=args.check_github,
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
