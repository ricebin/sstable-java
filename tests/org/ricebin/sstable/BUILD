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

java_test(
    name = "CppTableCompatTest",
    srcs = [
        "CppTableCompatTest.java",
    ],
    data = [
        "testfiles/000005.sst",
        "testfiles/testWithFilter.sst",
    ],
    test_class = "org.ricebin.sstable.CppTableCompatTest",
    deps = [
        "//main/org/ricebin/slice",
        "//main/org/ricebin/sstable",
        "@maven//:com_google_truth_truth_1_1_2",
    ]
)

java_test(
    name = "HashTest",
    srcs = [
        "HashTest.java",
    ],
    test_class = "org.ricebin.sstable.HashTest",
    deps = [
        "//main/org/ricebin/slice",
        "//main/org/ricebin/sstable",
        "@maven//:com_google_truth_truth_1_1_2",
    ]
)

java_test(
    name = "BloomFilterPolicyTest",
    srcs = [
        "BloomFilterPolicyTest.java",
    ],
    test_class = "org.ricebin.sstable.BloomFilterPolicyTest",
    deps = [
        "//main/org/ricebin/slice",
        "//main/org/ricebin/sstable",
        "@maven//:com_google_truth_truth_1_1_2",
    ]
)