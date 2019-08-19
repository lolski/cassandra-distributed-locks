java_library(
    name = "jackson-core-asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-core-asl"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-mapper-asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-mapper-asl"
    ],
    runtime_deps = [
        ":jackson-core-asl"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


