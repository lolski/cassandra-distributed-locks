java_library(
    name = "ohc-core",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc-core"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/google/guava:guava",
        "//dependencies/artifacts/net/java/dev/jna:jna",
        "//dependencies/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "ohc-core-j8",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc-core-j8"
    ],
    runtime_deps = [
        ":ohc-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


