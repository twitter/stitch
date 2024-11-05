junit_tests(
    sources = ["*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    runtime_platform = "java11",
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/scalatestplus:scalacheck",
        "stitch/stitch-core",
        "stitch/stitch-core/src/test/java/com/twitter/stitch",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/helpers",
        "util/util-core:scala",
    ],
)
