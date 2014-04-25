cmake_minimum_required (VERSION 2.8.0)
project (VeilHelpers)

# The version number.
set(VeilHelpers_VERSION_MAJOR 0)
set(VeilHelpers_VERSION_MINOR 2)
set(VeilHelpers_VERSION_PATCH 0)
set(VeilHelpers_VERSION $(VeilHelpers_VERSION_MAJOR).$(VeilHelpers_VERSION_MINOR).$(VeilHelpers_VERSION_PATCH))


# CMake config
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
add_definitions(-D_WEBSOCKETPP_CPP11_STL_)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
set(DEPS_DIR "deps")
link_directories(${PROJECT_BINARY_DIR})

string( TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_L )
if(CMAKE_BUILD_TYPE_L MATCHES "debug")
    set(VeilHelpers_DEBUG 1)
else()
    set(VeilHelpers_DEBUG 0)
endif()

configure_file (
    "${PROJECT_SOURCE_DIR}/include/veilConfig.h.in"
    "${PROJECT_BINARY_DIR}/veilConfig.h"
)

# Deps
add_subdirectory(${DEPS_DIR}/gmock)
add_subdirectory(${DEPS_DIR}/glog)
add_subdirectory(${DEPS_DIR}/websocketpp)

# Include paths
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_BINARY_DIR})
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${WSPP_INCLUDE_DIR})

# Setup git
message(STATUS "Checking for Git...")
find_package(Git REQUIRED)

# Setup ProtoBuf
message(STATUS "Checking for Protobuf...")
find_package(Protobuf REQUIRED)

if(${PREFER_STATIC_LINK})
    find_library(PROTOBUF_LIBS NAMES libprotobuf.a protobuf)
else()
    set(PROTOBUF_LIBS ${PROTOBUF_LIBRARY})
endif()

include_directories(${PROTOBUF_INCLUDE_DIR})


# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)

if(${PREFER_STATIC_LINK})

    find_library(SSL_LIBRARY NAMES libssl.a ssl)
    find_library(CRYPTO_LIBRARY NAMES libcrypto.a crypto)

    find_library(KRB5_LIBRARY krb5)
    if(${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)
        set(OPENSSL_LIBRARIES "${SSL_LIBRARY};${CRYPTO_LIBRARY}")
    else(${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)
        set(OPENSSL_LIBRARIES "${SSL_LIBRARY};${CRYPTO_LIBRARY};${KRB5_LIBRARY}")
    endif(${KRB5_LIBRARY} MATCHES KRB5_LIBRARY-NOTFOUND)

endif()

include_directories(${OPENSSL_INCLUDE_DIR})

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup fuse
message(STATUS "Checking for FUSE...")
find_package(FUSE REQUIRED)
include_directories(${FUSE_INCLUDE_DIRS})


# Setup Boost
set(Boost_USE_STATIC_LIBS       OFF)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS thread filesystem system random REQUIRED)

if(${PREFER_STATIC_LINK})

    set(BOOST_LIBS boost_thread boost_filesystem boost_system boost_random)

    foreach(LIB ${BOOST_LIBS})
        find_library(BOOST_${LIB}_LIBRARY NAMES lib${LIB}-mt.a lib${LIB}.a ${LIB}-mt ${LIB})
        if(${BOOST_${LIB}_LIBRARY} MATCHES .*_LIBRARY-NOTFOUND)
            message( FATAL_ERROR "ERROR: ${LIB} NOT FOUND!")
        endif()
        set(BOOST_DEPS ${BOOST_DEPS};${BOOST_${LIB}_LIBRARY})
    endforeach()
    set(Boost_LIBRARIES ${BOOST_DEPS})

endif()


link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})


# Utility libs
if(${PREFER_STATIC_LINK})
    find_library(LTDL_LIBRARY NAMES libltdl.a ltdl)
    find_library(ZLIB_LIBRARY NAMES libz.a z)
else()
    find_library(LTDL_LIBRARY ltdl)
    find_library(ZLIB_LIBRARY z)
endif()

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
file(GLOB PROTO_FILES "${PROJECT_SOURCE_DIR}/veilprotocol/proto/*.proto")
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Setup sources
file(GLOB HELPER_SOURCES "src/[a-zA-Z]*.cc")

# Setup compile flags
if(${PREFER_STATIC_LINK}) # Try to link libstdc++.a
    execute_process(COMMAND g++ -print-file-name=libstdc++.a OUTPUT_VARIABLE LIBSTD_NAME)
    string(REGEX REPLACE "(\r?\n)+$" "" LIBSTD_NAME "${LIBSTD_NAME}")
    execute_process(COMMAND ln -sf "${LIBSTD_NAME}" WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
endif()

set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/${LIB_DIR}:${CMAKE_CURRENT_BINARY_DIR}:.:${LIB_DIR}:../${LIB_DIR}:./c_lib:../c_lib:$ORIGIN")

if(APPLE)
    set(PLATFORM_EXTRA_LIBS "${CMAKE_THREAD_LIBS_INIT};${LTDL_LIBRARY};${ZLIB_LIBRARY};${DL_LIBRARY}")
    set(SECTION_FRAGMENTATION_FLAGS "-Wno-deprecated-declarations")
else()
    set(PLATFORM_EXTRA_LIBS "${CMAKE_THREAD_LIBS_INIT};${LTDL_LIBRARY};${ZLIB_LIBRARY};${DL_LIBRARY};${RT_LIBRARY}")
    set(SECTION_FRAGMENTATION_FLAGS "-fdata-sections -ffunction-sections -Wl,--gc-sections -Wl,-rpath,${CUSTOM_RPATH}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SECTION_FRAGMENTATION_FLAGS} -fPIC ${FUSE_DEFINITIONS} -DFUSE_USE_VERSION=29 -Wno-format")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG} -Wall -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE} -O2")

set(CMAKE_SKIP_RPATH TRUE) # Something is wrong with CMake RPATH config, we need to use old-fashion way


# Define targets

add_library(veilprotocol ${PROTO_SRCS})

add_library(veilhelpers_obj OBJECT ${HELPER_SOURCES})
add_library(veilhelpers STATIC $<TARGET_OBJECTS:veilhelpers_obj>)
add_library(veilhelpersShared SHARED $<TARGET_OBJECTS:veilhelpers_obj>)

add_dependencies(veilhelpers_obj glog veilprotocol websocketpp)

set_target_properties(veilhelpersShared PROPERTIES OUTPUT_NAME veilhelpers)

target_link_libraries(veilhelpers
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${PROTOBUF_LIBS}
    ${OPENSSL_LIBRARIES}
    ${GLOG_LIBRARIES}
    veilprotocol
    ${PLATFORM_EXTRA_LIBS}
)

target_link_libraries(veilhelpersShared
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${PROTOBUF_LIBS}
    ${OPENSSL_LIBRARIES}
    ${GLOG_LIBRARIES}
    veilprotocol
    ${PLATFORM_EXTRA_LIBS}
)

target_link_libraries(veilprotocol
    ${PROTOBUF_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
)


install(TARGETS veilhelpers veilhelpersShared
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
install(FILES ${GLOG_LIBRARIES} ${GTEST_LIBRARIES} ${GMOCK_LIBRARIES} DESTINATION lib)
install(DIRECTORY ${GTEST_INCLUDE_DIR}/gtest DESTINATION include)
install(DIRECTORY ${GMOCK_INCLUDE_DIR}/gmock DESTINATION include)
install(DIRECTORY ${GLOG_INCLUDE_DIR}/glog DESTINATION include)
message(STATUS ${GLOG_INCLUDE_DIR})

###########################
##         TESTS         ##
###########################

enable_testing()
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${GMOCK_INCLUDE_DIR})

add_custom_target(cunit)
macro(run_test test_target)
  string(REGEX REPLACE "_test" "" test_name "${test_target}")
  add_custom_target(${test_target}_runtest
      COMMAND ${test_target} --gtest_output="xml:cunit_results/TEST-${test_name}.xml"
      DEPENDS ${test_target}
      WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
  add_dependencies(cunit ${test_target}_runtest)
endmacro()

set(TEST_SOURCES_DIR "test")
include_directories(${TEST_SOURCES_DIR}/include)

file(GLOB files ${TEST_SOURCES_DIR}/*_test.cc)
foreach(file ${files})
    get_filename_component(TEST_NAME ${file} NAME_WE)
    add_executable(${TEST_NAME} ${file})
    add_dependencies(${TEST_NAME} gtest gmock veilFuse veilhelpers veilclient veilprotocol)
    target_link_libraries(${TEST_NAME}
        ${GTEST_LIBRARIES}
        ${Boost_LIBRARIES_SHARED}
        ${GMOCK_LIBRARIES}
        ${FUSE_LIBRARIES}
        veilhelpers
        ${PLATFORM_EXTRA_LIBS}
    )
    add_test(${TEST_NAME} ${TEST_NAME})
    run_test(${TEST_NAME})
endforeach()
