junit_tests(
    sources = ["*.scala"],
    runtime_platform = "java11",
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "stitch/stitch-core:cache",
        "stitch/stitch-core/src/main/scala/com/twitter/stitch/cache/caffeine",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/abstractsuites",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/helpers",
        "util/util-core:util-core-util",
        "util/util-core/src/main/scala/com/twitter/conversions",
    ],
)
