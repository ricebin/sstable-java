java_plugin(
    name = "BenchmarkProcessor",
    processor_class = "org.openjdk.jmh.generators.BenchmarkProcessor",
    deps = [
        "@maven//:org_openjdk_jmh_jmh_core_1_28",
        "@maven//:org_openjdk_jmh_jmh_generator_annprocess_1_28",
    ],
)

java_binary(
    name = "random_get_benchmark_runner",
    main_class = "org.openjdk.jmh.Main",
    srcs = [
        "RandomGetBenchmark.java",
    ],
    data = [
        "testfiles/000005.sst",
    ],
    deps = [
        "//main/org/ricebin/sstable",
        "//main/org/ricebin/slice",
        "@maven//:org_openjdk_jmh_jmh_core_1_28",
        "@maven//:org_openjdk_jmh_jmh_generator_annprocess_1_28",
    ],
    plugins = [
        ":BenchmarkProcessor",
    ]
)
