# CMake generated Testfile for 
# Source directory: /home/lichon/IdeaProjects/veilcluster/veilhelpers
# Build directory: /home/lichon/IdeaProjects/veilcluster/veilhelpers/release
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
ADD_TEST(fileCache_test "fileCache_test")
ADD_TEST(communicator_test "communicator_test")
ADD_TEST(communicationHandler_test "communicationHandler_test")
ADD_TEST(connectionPool_test "connectionPool_test")
ADD_TEST(connection_test "connection_test")
ADD_TEST(clusterProxyHelper_test "clusterProxyHelper_test")
ADD_TEST(storageHelperFactory_test "storageHelperFactory_test")
ADD_TEST(logging_test "logging_test")
SUBDIRS(deps/gmock)
SUBDIRS(deps/glog)
SUBDIRS(deps/websocketpp)
