java_library(
    name = "janusgraph-cassandra",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-cassandra"
    ],
    runtime_deps = [
        "//dependencies/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/artifacts/com/netflix/astyanax:astyanax-cassandra",
        "//dependencies/artifacts/com/netflix/astyanax:astyanax-core",
        "//dependencies/artifacts/com/netflix/astyanax:astyanax-recipes",
        "//dependencies/artifacts/com/netflix/astyanax:astyanax-thrift",
        "//dependencies/artifacts/commons-pool:commons-pool",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-jvm",
        "//dependencies/artifacts/javax/validation:validation-api",
        "//dependencies/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/xerial/snappy:snappy-java",
        ":janusgraph-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "janusgraph-core",
    exports = [
        "//external:jar/org/janusgraph/janusgraph-core"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/carrotsearch:hppc",
        "//dependencies/artifacts/com/github/stephenc/high-scale-lib:high-scale-lib",
        "//dependencies/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/artifacts/com/google/guava:guava",
        "//dependencies/artifacts/commons-codec:commons-codec",
        "//dependencies/artifacts/commons-collections:commons-collections",
        "//dependencies/artifacts/commons-configuration:commons-configuration",
        "//dependencies/artifacts/commons-io:commons-io",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-ganglia",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-graphite",
        "//dependencies/artifacts/org/apache/commons:commons-text",
        "//dependencies/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/artifacts/org/apache/tinkerpop:gremlin-groovy",
        "//dependencies/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/artifacts/org/glassfish:javax-json",
        "//dependencies/artifacts/org/locationtech/jts:jts-core",
        "//dependencies/artifacts/org/locationtech/spatial4j:spatial4j",
        "//dependencies/artifacts/org/noggit:noggit",
        "//dependencies/artifacts/org/reflections:reflections",
        "//dependencies/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//visibility:public"
    ]
)


