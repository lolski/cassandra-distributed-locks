java_test(
    name = "reentrant-lock",
    test_class = "distributedlock.ReentrantLockTest",
    srcs = ["ReentrantLockTest.java", "Count.java"],
    deps = [
        "//dependencies/artifacts/com/datastax/cassandra:cassandra-driver-core",
    ],
)

java_test(
    name = "cassandra-fenced-lock",
    test_class = "distributedlock.CassandraFencedLockTest",
    srcs = ["CassandraFencedLockTest.java", "Count.java"],
    deps = [
        "//dependencies/artifacts/com/datastax/cassandra:cassandra-driver-core",
    ],
)