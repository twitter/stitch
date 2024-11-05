alias(
    name = "stitch-core",
    target = "stitch/stitch-core/src/main/scala/com/twitter/stitch",
)

alias(
    name = "cache",
    target = "stitch/stitch-core/src/main/scala/com/twitter/stitch/cache",
)

test_suite(
    name = "tests",
    tags = ["bazel-compatible"],
    dependencies = [
        "stitch/stitch-core/src/test/scala/com/twitter/stitch",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/caffeine",
    ],
)

target(
    name = "test-tools",
    dependencies = [
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/abstractsuites",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/cache/testable",
        "stitch/stitch-core/src/test/scala/com/twitter/stitch/helpers",
    ],
)
