scala_library(
    sources = ["*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "stitch-cache-caffeine",
        repo = artifactory,
    ),
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "stitch/stitch-core",
        "util/util-core:scala",
    ],
    exports = [
        "stitch/stitch-core",
    ],
)
