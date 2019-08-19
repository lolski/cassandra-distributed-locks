java_library(
    name = "reporter-config-base",
    exports = [
        "//external:jar/com/addthis/metrics/reporter-config-base"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/artifacts/org/hibernate:hibernate-validator",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/yaml:snakeyaml"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "reporter-config3",
    exports = [
        "//external:jar/com/addthis/metrics/reporter-config3"
    ],
    runtime_deps = [
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/artifacts/org/hibernate:hibernate-validator",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/yaml:snakeyaml",
        ":reporter-config-base"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


