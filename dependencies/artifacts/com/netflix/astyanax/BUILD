java_library(
    name = "astyanax-cassandra",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax-cassandra"
    ],
    runtime_deps = [
        "//dependencies/artifacts/commons-codec:commons-codec",
        "//dependencies/artifacts/org/apache/servicemix/bundles:org-apache-servicemix-bundles-commons-csv",
        "//dependencies/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/artifacts/org/codehaus/jettison:jettison",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/xerial/snappy:snappy-java",
        ":astyanax-cassandra-all-shaded",
        ":astyanax-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "astyanax-cassandra-all-shaded",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax-cassandra-all-shaded"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "astyanax-core",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax-core"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/eaio/uuid:uuid",
        "//dependencies/artifacts/com/github/stephenc/high-scale-lib:high-scale-lib",
        "//dependencies/artifacts/com/google/guava:guava",
        "//dependencies/artifacts/commons-lang:commons-lang",
        "//dependencies/artifacts/joda-time:joda-time",
        "//dependencies/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "astyanax-recipes",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax-recipes"
    ],
    runtime_deps = [
        ":astyanax-cassandra",
        ":astyanax-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "astyanax-thrift",
    exports = [
        "//external:jar/com/netflix/astyanax/astyanax-thrift"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/apache/cassandra:cassandra-thrift",
        ":astyanax-cassandra",
        ":astyanax-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


