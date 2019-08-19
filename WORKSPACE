workspace(name = "distkv")



###############################################################
#                   common bazel imports                      #
###############################################################
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_jar")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")



###############################################################
#            maven dependencies with bazel-deps               #
###############################################################
http_jar(
    name = "bazel_deps",
    sha256 = "43278a0042e253384543c4700021504019c1f51f3673907a1b25bb1045461c0c",
    urls = ["https://github.com/graknlabs/bazel-deps/releases/download/v0.2/grakn-bazel-deps-v0.2.jar"],
)

load("//dependencies:dependencies.bzl", "maven_dependencies")
maven_dependencies()



###############################################################
#                        bazel-distribution                   #
###############################################################

git_repository(
    name = "graknlabs_bazel_distribution",
    remote = "https://github.com/graknlabs/bazel-distribution",
    commit = "148fdc3912e4c40bb27ebe8a1a4a9e36d9bfd1bf" # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_bazel_distribution
)