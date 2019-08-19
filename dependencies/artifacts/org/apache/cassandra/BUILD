java_library(
    name = "cassandra-all",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra-all"
    ],
    runtime_deps = [
        "//dependencies/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/artifacts/ch/qos/logback:logback-core",
        "//dependencies/artifacts/com/addthis/metrics:reporter-config3",
        "//dependencies/artifacts/com/boundary:high-scale-lib",
        "//dependencies/artifacts/com/clearspring/analytics:stream",
        "//dependencies/artifacts/com/github/ben-manes/caffeine:caffeine",
        "//dependencies/artifacts/com/github/jbellis:jamm",
        "//dependencies/artifacts/com/google/guava:guava",
        "//dependencies/artifacts/com/googlecode/concurrentlinkedhashmap:concurrentlinkedhashmap-lru",
        "//dependencies/artifacts/com/googlecode/json-simple:json-simple",
        "//dependencies/artifacts/com/ning:compress-lzf",
        "//dependencies/artifacts/com/thinkaurelius/thrift:thrift-server",
        "//dependencies/artifacts/commons-cli:commons-cli",
        "//dependencies/artifacts/commons-codec:commons-codec",
        "//dependencies/artifacts/io/airlift:airline",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/artifacts/io/dropwizard/metrics:metrics-jvm",
        "//dependencies/artifacts/io/netty:netty-all",
        "//dependencies/artifacts/joda-time:joda-time",
        "//dependencies/artifacts/net/java/dev/jna:jna",
        "//dependencies/artifacts/net/jpountz/lz4:lz4",
        "//dependencies/artifacts/org/antlr:antlr",
        "//dependencies/artifacts/org/antlr:antlr-runtime",
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/artifacts/org/apache/commons:commons-math3",
        "//dependencies/artifacts/org/apache/thrift:libthrift",
        "//dependencies/artifacts/org/caffinitas/ohc:ohc-core",
        "//dependencies/artifacts/org/caffinitas/ohc:ohc-core-j8",
        "//dependencies/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/artifacts/org/eclipse/jdt/core/compiler:ecj",
        "//dependencies/artifacts/org/fusesource:sigar",
        "//dependencies/artifacts/org/jctools:jctools-core",
        "//dependencies/artifacts/org/mindrot:jbcrypt",
        "//dependencies/artifacts/org/ow2/asm:asm",
        "//dependencies/artifacts/org/slf4j:jcl-over-slf4j",
        "//dependencies/artifacts/org/slf4j:log4j-over-slf4j",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/xerial/snappy:snappy-java",
        "//dependencies/artifacts/org/yaml:snakeyaml",
        ":cassandra-thrift"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "cassandra-thrift",
    exports = [
        "//external:jar/org/apache/cassandra/cassandra-thrift"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/carrotsearch:hppc",
        "//dependencies/artifacts/com/github/rholder:snowball-stemmer",
        "//dependencies/artifacts/com/googlecode/concurrent-trees:concurrent-trees",
        "//dependencies/artifacts/de/jflex:jflex",
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/artifacts/org/apache/thrift:libthrift",
        "//dependencies/artifacts/org/slf4j:jcl-over-slf4j",
        "//dependencies/artifacts/org/slf4j:log4j-over-slf4j",
        "//dependencies/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


