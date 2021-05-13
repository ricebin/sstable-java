load("@rules_java//java:defs.bzl", "java_test")

java_test(
    name = "TableTest",
    srcs = [
        "TableTest.java",
    ],
    test_class = "org.ricebin.sstable.TableTest",
    deps = [
        "//main/org/ricebin/slice",
        "//main/org/ricebin/sstable",
        "@maven//:com_google_truth_truth_1_1_2",
    ]
)

