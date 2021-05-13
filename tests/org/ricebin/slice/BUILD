load("@rules_java//java:defs.bzl", "java_test")

java_test(
    name = "ByteBufferSliceTest",
    srcs = [
        "ByteBufferSliceTest.java",
    ],
    test_class = "org.ricebin.slice.ByteBufferSliceTest",
    deps = [
        "//main/org/ricebin/slice",
        "@maven//:com_google_truth_truth_1_1_2",
    ]
)