java_library(
    name = "jcabi-aspects",
    exports = [
        "//external:jar/com/jcabi/jcabi-aspects"
    ],
    runtime_deps = [
        "//dependencies/artifacts/javax/validation:validation-api",
        "//dependencies/artifacts/org/aspectj:aspectjrt"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jcabi-log",
    exports = [
        "//external:jar/com/jcabi/jcabi-log"
    ],
    runtime_deps = [
        ":jcabi-aspects"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jcabi-manifests",
    exports = [
        "//external:jar/com/jcabi/jcabi-manifests"
    ],
    runtime_deps = [
        ":jcabi-log"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


