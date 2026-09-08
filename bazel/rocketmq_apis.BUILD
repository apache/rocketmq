# Build file for the rocketmq-apis git submodule.
#
# The submodule ships its own BUILD files (root + java/ + cpp/), but those pull
# in toolchains this workspace does not declare (graknlabs_bazel_distribution,
# googleapis). Rather than dragging all of that in, the directory is listed in
# .bazelignore (so `bazel build //...` never tries to build it) and is exposed to
# this workspace as the external repository @rocketmq_apis through a minimal
# build file instead.
#
# The proto set is a glob over the whole v2 directory, so admin.proto (RIP-2) is
# included alongside definition.proto and service.proto.
filegroup(
    name = "v2_protos",
    srcs = glob(["apache/rocketmq/v2/*.proto"]),
    visibility = ["//visibility:public"],
)
