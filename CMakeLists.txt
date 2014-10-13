cmake_minimum_required(VERSION 3.0.0)
project(helpers C CXX)

# The version number.
set(helpers_VERSION_MAJOR 0)
set(helpers_VERSION_MINOR 2)
set(helpers_VERSION_PATCH 0)
set(helpers_VERSION $(helpers_VERSION_MAJOR).$(helpers_VERSION_MINOR).$(helpers_VERSION_PATCH))

# CMake config
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
link_directories(${PROJECT_BINARY_DIR})

configure_file (
    include/version.h.in
    ${PROJECT_BINARY_DIR}/version.h
)

# Deps
add_subdirectory(deps/gmock)
add_subdirectory(deps/glog)
add_subdirectory(deps/websocketpp)

# Include paths
include_directories(include ${PROJECT_BINARY_DIR})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR} ${WSPP_INCLUDE_DIR})

# Setup git
message(STATUS "Checking for Git...")
find_package(Git REQUIRED)

# Setup ProtoBuf
message(STATUS "Checking for Protobuf...")
find_package(Protobuf REQUIRED)

if(${PREFER_STATIC_LINK})
    find_library(PROTOBUF_LIBRARY NAMES libprotobuf.a protobuf)
endif(${PREFER_STATIC_LINK})

include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)

if(${PREFER_STATIC_LINK})
    find_library(SSL_LIBRARY NAMES libssl.a ssl)
    find_library(CRYPTO_LIBRARY NAMES libcrypto.a crypto)
    find_library(KRB5_LIBRARY krb5)

    if(${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)
        set(OPENSSL_LIBRARIES ${SSL_LIBRARY} ${CRYPTO_LIBRARY})
    else(NOT ${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)
        set(OPENSSL_LIBRARIES ${SSL_LIBRARY} ${CRYPTO_LIBRARY} ${KRB5_LIBRARY})
    endif(${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)
endif(${PREFER_STATIC_LINK})

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup fuse
message(STATUS "Checking for FUSE...")
find_package(FUSE REQUIRED)
include_directories(SYSTEM ${FUSE_INCLUDE_DIRS})

# Setup Boost
if(${PREFER_STATIC_LINK})
    set(Boost_USE_STATIC_LIBS       ON)
else(NOT ${PREFER_STATIC_LINK})
    set(Boost_USE_STATIC_LIBS       OFF)
endif(${PREFER_STATIC_LINK})

set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS thread filesystem system random REQUIRED)

link_directories(${Boost_LIBRARY_DIRS})
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

# Utility libs
if(${PREFER_STATIC_LINK})
    find_library(LTDL_LIBRARY NAMES libltdl.a ltdl)
    find_library(ZLIB_LIBRARY NAMES libz.a z)
else(NOT ${PREFER_STATIC_LINK})
    find_library(LTDL_LIBRARY ltdl)
    find_library(ZLIB_LIBRARY z)
endif(${PREFER_STATIC_LINK})

find_library(RT_LIBRARY rt)
find_library(DL_LIBRARY dl)

# Pull git submodules
message(STATUS "Pulling submodules using git...")
execute_process(COMMAND ${GIT_EXECUTABLE} submodule init
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
execute_process(COMMAND ${GIT_EXECUTABLE} submodule update
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
execute_process(COMMAND ${GIT_EXECUTABLE} submodule foreach git checkout
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

# Compile .proto files
file(GLOB PROTO_FILES clproto/proto/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Setup sources
file(GLOB_RECURSE HELPER_SOURCES src/*.cc include/*.h)

# Setup compile flags
if(${PREFER_STATIC_LINK}) # Try to link libstdc++.a
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libstdc++.a OUTPUT_VARIABLE LIBSTD_NAME)
    string(STRIP ${LIBSTD_NAME} LIBSTD_NAME)
    if(EXISTS ${LIBSTD_NAME})
        execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${LIBSTD_NAME} ${PROJECT_BINARY_DIR}/libstdc++.a)
    endif(EXISTS ${LIBSTD_NAME})
endif(${PREFER_STATIC_LINK})

set(PLATFORM_EXTRA_LIBS ${CMAKE_THREAD_LIBS_INIT} ${LTDL_LIBRARY} ${ZLIB_LIBRARY} ${DL_LIBRARY})

if(APPLE)
    set(SECTION_FRAGMENTATION_FLAGS -Wno-deprecated-declarations)
    set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
else(NOT APPLE)
    set(PLATFORM_EXTRA_LIBS ${PLATFORM_EXTRA_LIBS} ${RT_LIBRARY})
    set(SECTION_FRAGMENTATION_FLAGS -fdata-sections -ffunction-sections)
    set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/lib" "\$ORIGIN")
endif(APPLE)

add_compile_options(${SECTION_FRAGMENTATION_FLAGS} -Wall -fPIC -std=c++14)
add_definitions(${FUSE_DEFINITIONS} -DFUSE_USE_VERSION=29 -D_WEBSOCKETPP_CPP11_STL_ -DBOOST_BIND_NO_PLACEHOLDERS)

# Define targets
add_library(clproto STATIC ${PROTO_SRCS})
add_library(helpers_obj OBJECT ${HELPER_SOURCES})
add_library(helpers STATIC $<TARGET_OBJECTS:helpers_obj>)
add_library(helpersShared SHARED $<TARGET_OBJECTS:helpers_obj>)

add_dependencies(helpers_obj glog clproto websocketpp)
set_target_properties(helpersShared PROPERTIES
    OUTPUT_NAME helpers
    BUILD_WITH_INSTALL_RPATH true
    INSTALL_RPATH_USE_LINK_PATH true
    INSTALL_RPATH "${CUSTOM_RPATH}")

set(COMMON_LIBRARIES
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${GLOG_LIBRARIES}
    clproto
    ${PLATFORM_EXTRA_LIBS}
)

target_link_libraries(helpers ${COMMON_LIBRARIES})
target_link_libraries(helpersShared ${COMMON_LIBRARIES})
target_link_libraries(clproto ${PROTOBUF_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS helpers helpersShared clproto
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
install(FILES ${GLOG_LIBRARIES} ${GTEST_LIBRARIES} ${GMOCK_LIBRARIES} DESTINATION lib)
install(DIRECTORY ${GTEST_INCLUDE_DIR}/gtest DESTINATION include)
install(DIRECTORY ${GMOCK_INCLUDE_DIR}/gmock DESTINATION include)
install(DIRECTORY ${GLOG_INCLUDE_DIR}/glog DESTINATION include)

###########################
##         TESTS         ##
###########################

enable_testing()
include_directories(test/include)
include_directories(SYSTEM ${GTEST_INCLUDE_DIR} ${GMOCK_INCLUDE_DIR})

file(GLOB_RECURSE HELPER_TEST_HEADERS test/include/*.h)
add_library(testRunner OBJECT test/testRunner.cc)
add_dependencies(testRunner glog gmock gtest)

add_custom_target(cunit)
macro(run_test test_target)
  string(REGEX REPLACE "_test" "" test_name "${test_target}")
  add_custom_target(${test_target}_runtest
      COMMAND ${test_target} --gtest_output="xml:cunit_results/TEST-${test_name}.xml"
      DEPENDS ${test_target}
      WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_dependencies(cunit ${test_target}_runtest)
endmacro()

file(GLOB_RECURSE HELPER_TEST_SOURCES test/*_test.cc)
foreach(TEST_FILE ${HELPER_TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
    add_executable(${TEST_NAME} ${TEST_FILE} ${HELPER_TEST_HEADERS} $<TARGET_OBJECTS:testRunner>)
    add_dependencies(${TEST_NAME} gmock gtest)
    target_link_libraries(${TEST_NAME}
        ${GTEST_LIBRARIES}
        ${Boost_LIBRARIES_SHARED}
        ${GMOCK_LIBRARIES}
        ${FUSE_LIBRARIES}
        helpers
        ${PLATFORM_EXTRA_LIBS}
    )
    add_test(${TEST_NAME} ${TEST_NAME})
    run_test(${TEST_NAME})
endforeach()
