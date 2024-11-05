junit_tests(
    sources = [
        "*.scala",
    ],
    runtime_platform = "java11",
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/mockito:mockito-core",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "stitch/stitch-core:cache",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/abstractsuites",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/testable",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/helpers",
        "util/util-core:scala",
        "util/util-mock/src/main/scala/com/twitter/util/mock",
    ],
)
