java_library(
    name = "httpclient",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpclient"
    ],
    runtime_deps = [
        "//dependencies/artifacts/commons-codec:commons-codec",
        "//dependencies/artifacts/commons-logging:commons-logging",
        ":httpcore"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "httpcore",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


