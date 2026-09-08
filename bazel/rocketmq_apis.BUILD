# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
