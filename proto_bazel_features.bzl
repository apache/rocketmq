# Vendored proto_bazel_features repository rule.
# Avoids a dependency cycle: loading from @com_google_protobuf requires
# proto_bazel_features to already exist in the repo mapping.
# Source: https://github.com/protocolbuffers/protobuf (bazel/private/oss/proto_bazel_features.bzl)

_PROTO_BAZEL_FEATURES = """bazel_features = struct(
  cc = struct(
    protobuf_on_allowlist = {protobuf_on_allowlist},
  ),
  proto = struct(
    starlark_proto_info = {starlark_proto_info},
  ),
  rules = struct(
    analysis_tests_can_transition_on_experimental_incompatible_flags = {analysis_tests_can_transition_on_experimental_incompatible_flags},
  ),
  globals = struct(
    PackageSpecificationInfo = {PackageSpecificationInfo},
    ProtoInfo = getattr(getattr(native, 'legacy_globals', None), 'ProtoInfo', {ProtoInfo}),
    cc_proto_aspect = getattr(getattr(native, 'legacy_globals', None), 'cc_proto_aspect', {cc_proto_aspect}),
  ),
)
"""

def _proto_bazel_features_impl(rctx):
    bazel_version = native.bazel_version or "999999.999999.999999"
    version_parts = bazel_version.split("-")[0].split(".")
    if len(version_parts) != 3:
        fail("invalid Bazel version '{}': got {} dot-separated segments, want 3".format(bazel_version, len(version_parts)))
    major_version_int = int(version_parts[0])
    minor_version_int = int(version_parts[1])

    starlark_proto_info = major_version_int >= 7
    PackageSpecificationInfo = major_version_int > 6 or (major_version_int == 6 and minor_version_int >= 4)

    protobuf_on_allowlist = major_version_int > 7
    ProtoInfo = "ProtoInfo" if major_version_int < 8 else "None"
    cc_proto_aspect = "cc_proto_aspect" if major_version_int < 8 else "None"

    rctx.file("BUILD.bazel", """
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
bzl_library(
    name = "features",
    srcs = ["features.bzl"],
    visibility = ["//visibility:public"],
)
exports_files(["features.bzl"])
""")
    rctx.file("features.bzl", _PROTO_BAZEL_FEATURES.format(
        starlark_proto_info = repr(starlark_proto_info),
        PackageSpecificationInfo = "PackageSpecificationInfo" if PackageSpecificationInfo else "None",
        protobuf_on_allowlist = repr(protobuf_on_allowlist),
        ProtoInfo = ProtoInfo,
        cc_proto_aspect = cc_proto_aspect,
        analysis_tests_can_transition_on_experimental_incompatible_flags =
            "True" if major_version_int > 8 or (major_version_int == 8 and minor_version_int >= 2) else "False",
    ))

proto_bazel_features = repository_rule(
    implementation = _proto_bazel_features_impl,
    local = True,
)
