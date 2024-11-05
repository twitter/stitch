scala_library(
    sources = ["*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "stitch-cache",
        repo = artifactory,
    ),
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "stitch/stitch-core",
        "util/util-core:scala",
    ],
    exports = [
        "stitch/stitch-core",
    ],
)
