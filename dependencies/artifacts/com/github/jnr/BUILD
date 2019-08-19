java_library(
    name = "jffi",
    exports = [
        "//external:jar/com/github/jnr/jffi"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jffi-jar-native",
    exports = [
        "//external:jar/com/github/jnr/jffi-jar-native"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr-constants",
    exports = [
        "//external:jar/com/github/jnr/jnr-constants"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr-ffi",
    exports = [
        "//external:jar/com/github/jnr/jnr-ffi"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/ow2/asm:asm",
        "//dependencies/artifacts/org/ow2/asm:asm-analysis",
        "//dependencies/artifacts/org/ow2/asm:asm-commons",
        "//dependencies/artifacts/org/ow2/asm:asm-tree",
        "//dependencies/artifacts/org/ow2/asm:asm-util",
        ":jffi",
        ":jffi-jar-native",
        ":jnr-x86asm"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr-posix",
    exports = [
        "//external:jar/com/github/jnr/jnr-posix"
    ],
    runtime_deps = [
        ":jnr-constants",
        ":jnr-ffi"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr-x86asm",
    exports = [
        "//external:jar/com/github/jnr/jnr-x86asm"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


