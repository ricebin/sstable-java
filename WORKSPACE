workspace(name = "ricebin_leveldb_java")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.0"
RULES_JVM_EXTERNAL_SHA = "31701ad93dbfe544d597dbe62c9a1fdd76d81d8a9150c2bf1ecf928ecdf97169"

http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "com.google.guava:guava:30.0-jre",
        "com.google.truth:truth:1.1.2",
        "net.jcip:jcip-annotations:1.0",
        "org.openjdk.jmh:jmh-core:1.28",
        "org.openjdk.jmh:jmh-generator-annprocess:1.28",
    ],
    repositories = [
        "https://jcenter.bintray.com",
        "https://repo1.maven.org/maven2",
        "https://maven.google.com",
    ]
)
