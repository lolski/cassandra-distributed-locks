java_library(
    name = "metrics-core",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-core"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-ganglia",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-ganglia"
    ],
    runtime_deps = [
        "//dependencies/artifacts/info/ganglia/gmetric4j:gmetric4j",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-graphite",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-graphite"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-jvm",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-jvm"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


