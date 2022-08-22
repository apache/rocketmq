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
        "junit:junit:4.12",
        "com.alibaba:fastjson:1.2.76",
        "org.hamcrest:hamcrest-library:1.3",
        "io.netty:netty-all:4.1.65.Final",
        "org.slf4j:slf4j-api:1.7.7",
        "org.assertj:assertj-core:2.6.0",
        "org.mockito:mockito-core:3.10.0",
        "com.github.luben:zstd-jni:1.5.2-2",
        "org.lz4:lz4-java:1.8.0",
        "commons-validator:commons-validator:1.7",
        "org.apache.commons:commons-lang3:3.4",
        "org.hamcrest:hamcrest-core:1.3",
      # "io.openmessaging.storage:dledger:0.2.4",
        "net.java.dev.jna:jna:4.2.2",
        "ch.qos.logback:logback-classic:1.2.10",
        "ch.qos.logback:logback-core:1.2.10",
        "io.opentracing:opentracing-api:0.33.0",
        "io.opentracing:opentracing-mock:0.33.0",
        "commons-collections:commons-collections:3.2.2",
        "org.awaitility:awaitility:4.1.0",
        "commons-cli:commons-cli:1.2",
        "com.google.guava:guava:31.0.1-jre",
        "org.yaml:snakeyaml:1.30",
        "commons-codec:commons-codec:1.9",
        "log4j:log4j:1.2.17",
        "com.google.truth:truth:0.30",
        "org.bouncycastle:bcpkix-jdk15on:1.69"
    ],
    fetch_sources = True,
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        "https://repo1.maven.org/maven2",
    ],
)

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "a2a5cccec251211e2221b1587af2ce43c36d32a42f5d881737db3b546a536510",
    strip_prefix = "buildbuddy-toolchain-829c8a574f706de5c96c54ca310f139f4acda7dd",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/829c8a574f706de5c96c54ca310f139f4acda7dd.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rbe_default",
    # sha256 = "c0d428774cbe70d477e1d07581d863f8dbff4ba6a66d20502d7118354a814bea",
    urls = ["https://storage.googleapis.com/rbe-toolchain/bazel-configs/rbe-ubuntu1604/latest/rbe_default.tar"],
)

http_archive(
	name = "bazel_toolchains",
	urls = ["https://github.com/bazelbuild/bazel-toolchains/archive/dac71231098d891e5c4b74a2078fe9343feef510.tar.gz"],
	strip_prefix = "bazel-toolchains-dac71231098d891e5c4b74a2078fe9343feef510",
	sha256 = "56d5370eb99559b4c74f334f81bc8a298f728bd16d5a4333c865c2ad10fae3bc",
)

load("@bazel_toolchains//repositories:repositories.bzl", bazel_toolchains_repositories = "repositories")
bazel_toolchains_repositories()
