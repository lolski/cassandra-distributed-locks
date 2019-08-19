java_library(
    name = "gremlin-core",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-core"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/carrotsearch:hppc",
        "//dependencies/artifacts/com/jcabi:jcabi-manifests",
        "//dependencies/artifacts/com/squareup:javapoet",
        "//dependencies/artifacts/commons-collections:commons-collections",
        "//dependencies/artifacts/commons-configuration:commons-configuration",
        "//dependencies/artifacts/net/objecthunter:exp4j",
        "//dependencies/artifacts/org/javatuples:javatuples",
        "//dependencies/artifacts/org/slf4j:jcl-over-slf4j",
        "//dependencies/artifacts/org/slf4j:slf4j-api",
        "//dependencies/artifacts/org/yaml:snakeyaml",
        ":gremlin-shaded"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "gremlin-groovy",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-groovy"
    ],
    runtime_deps = [
        "//dependencies/artifacts/com/github/ben-manes/caffeine:caffeine",
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/artifacts/org/apache/ivy:ivy",
        "//dependencies/artifacts/org/codehaus/groovy:groovy-groovysh-jar-indy",
        "//dependencies/artifacts/org/codehaus/groovy:groovy-jar-indy",
        "//dependencies/artifacts/org/codehaus/groovy:groovy-json-jar-indy",
        "//dependencies/artifacts/org/codehaus/groovy:groovy-jsr223-jar-indy",
        "//dependencies/artifacts/org/mindrot:jbcrypt",
        ":gremlin-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "gremlin-shaded",
    exports = [
        "//external:jar/org/apache/tinkerpop/gremlin-shaded"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tinkergraph-gremlin",
    exports = [
        "//external:jar/org/apache/tinkerpop/tinkergraph-gremlin"
    ],
    runtime_deps = [
        "//dependencies/artifacts/org/apache/commons:commons-lang3",
        ":gremlin-core"
    ],
    visibility = [
        "//dependencies/artifacts:__subpackages__"
    ]
)


