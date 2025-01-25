#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.2"

RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "junit:junit:4.13.2",
        "com.alibaba:fastjson:1.2.83",
        "com.alibaba.fastjson2:fastjson2:2.0.43",
        "org.hamcrest:hamcrest-library:1.3",
        "io.netty:netty-all:4.1.65.Final",
        "org.assertj:assertj-core:3.22.0",
        "org.mockito:mockito-core:3.10.0",
        "org.powermock:powermock-module-junit4:2.0.9",
        "org.powermock:powermock-api-mockito2:2.0.9",
        "org.powermock:powermock-core:2.0.9",
        "com.github.luben:zstd-jni:1.5.2-2",
        "org.lz4:lz4-java:1.8.0",
        "commons-validator:commons-validator:1.7",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.hamcrest:hamcrest-core:1.3",
        "io.openmessaging.storage:dledger:0.3.1",
        "net.java.dev.jna:jna:4.2.2",
        "ch.qos.logback:logback-classic:1.2.10",
        "ch.qos.logback:logback-core:1.2.10",
        "io.opentracing:opentracing-api:0.33.0",
        "io.opentracing:opentracing-mock:0.33.0",
        "commons-collections:commons-collections:3.2.2",
        "org.awaitility:awaitility:4.1.0",
        "commons-cli:commons-cli:1.5.0",
        "com.google.guava:guava:31.0.1-jre",
        "org.yaml:snakeyaml:1.30",
        "commons-codec:commons-codec:1.13",
        "commons-io:commons-io:2.7",
        "com.google.truth:truth:0.30",
        "org.bouncycastle:bcpkix-jdk15on:1.69",
        "com.google.code.gson:gson:2.8.9",
        "com.googlecode.concurrentlinkedhashmap:concurrentlinkedhashmap-lru:1.4.2",
        "org.apache.rocketmq:rocketmq-proto:2.0.4",
        "com.google.protobuf:protobuf-java:3.20.1",
        "com.google.protobuf:protobuf-java-util:3.20.1",
        "com.conversantmedia:disruptor:1.2.10",
        "org.apache.tomcat:annotations-api:6.0.53",
        "com.google.code.findbugs:jsr305:3.0.2",
        "org.checkerframework:checker-qual:3.12.0",
        "org.reflections:reflections:0.9.11",
        "org.openjdk.jmh:jmh-core:1.19",
        "org.openjdk.jmh:jmh-generator-annprocess:1.19",
        "com.github.ben-manes.caffeine:caffeine:2.9.3",
        "io.grpc:grpc-services:1.47.0",
        "io.grpc:grpc-netty-shaded:1.47.0",
        "io.grpc:grpc-context:1.47.0",
        "io.grpc:grpc-stub:1.47.0",
        "io.grpc:grpc-api:1.47.0",
        "io.grpc:grpc-testing:1.47.0",
        "org.springframework:spring-core:5.3.26",
        "io.opentelemetry:opentelemetry-exporter-otlp:1.29.0",
        "io.opentelemetry:opentelemetry-exporter-prometheus:1.29.0-alpha",
        "io.opentelemetry:opentelemetry-exporter-logging:1.29.0",
        "io.opentelemetry:opentelemetry-sdk:1.29.0",
        "io.opentelemetry:opentelemetry-exporter-logging-otlp:1.29.0",
        "com.squareup.okio:okio-jvm:3.0.0",
        "io.opentelemetry:opentelemetry-api:1.29.0",
        "io.opentelemetry:opentelemetry-sdk-metrics:1.29.0",
        "io.opentelemetry:opentelemetry-sdk-common:1.29.0",
        "io.github.aliyunmq:rocketmq-slf4j-api:1.0.0",
        "io.github.aliyunmq:rocketmq-logback-classic:1.0.0",
        "org.slf4j:jul-to-slf4j:2.0.6",
    	"org.jetbrains:annotations:23.1.0",
        "io.github.aliyunmq:rocketmq-shaded-slf4j-api-bridge:1.0.0",
        "software.amazon.awssdk:s3:2.20.29",
        "com.fasterxml.jackson.core:jackson-databind:2.13.4.2",
        "com.adobe.testing:s3mock-junit4:2.11.0",
        "io.github.aliyunmq:rocketmq-grpc-netty-codec-haproxy:1.0.0",
        "org.apache.rocketmq:rocketmq-rocksdb:1.0.2",
        "com.alipay.sofa:jraft-core:1.3.14",
        "com.alipay.sofa:hessian:3.3.6",
        "io.netty:netty-tcnative-boringssl-static:2.0.48.Final",
        "org.mockito:mockito-junit-jupiter:4.11.0",
        "com.alibaba.fastjson2:fastjson2:2.0.43",
        "org.junit.jupiter:junit-jupiter-api:5.9.1",
    ],
    fetch_sources = True,
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        "https://repo1.maven.org/maven2",
    ],
)

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "b12273608db627eb14051eb75f8a2134590172cd69392086d392e25f3954ea6e",
    strip_prefix = "buildbuddy-toolchain-8d5d18373adfca9d8e33b4812915abc9b132f1ee",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/8d5d18373adfca9d8e33b4812915abc9b132f1ee.tar.gz"],
)
load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")
buildbuddy_deps()
load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")
buildbuddy(name = "buildbuddy_toolchain")

http_archive(
    name = "bazel_toolchains",
    sha256 = "1adf5db506a7e3c465a26988514cfc3971af6d5b3c2218925cd6e71ee443fc3f",
    strip_prefix = "bazel-toolchains-4.0.0",
    urls = [
        "https://github.com/bazelbuild/bazel-toolchains/releases/download/4.0.0/bazel-toolchains-4.0.0.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/releases/download/4.0.0/bazel-toolchains-4.0.0.tar.gz",
    ],
)
