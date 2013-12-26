cmake_minimum_required (VERSION 2.8.0)
project (VeilHelpers)

# The version number.
set(VeilHelpers_VERSION_MAJOR 0)
set(VeilHelpers_VERSION_MINOR 1)
set(VeilHelpers_VERSION_PATCH 0)
set(VeilHelpers_VERSION $(VeilHelpers_VERSION_MAJOR).$(VeilHelpers_VERSION_MINOR).$(VeilHelpers_VERSION_PATCH))


# CMake config
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
set(DEPS_DIR "deps")
link_directories(${PROJECT_BINARY_DIR})

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
include_directories(${PROTOBUF_INCLUDE_DIR})

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup fuse
message(STATUS "Checking for FUSE...")
find_package(FUSE REQUIRED)
include_directories(${FUSE_INCLUDE_DIRS})

# Setup Boost
set(Boost_USE_STATIC_LIBS        ON)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS system thread filesystem random REQUIRED)
set(Boost_LIBRARIES             "boost_thread-mt.a;boost_filesystem-mt.a;boost_system-mt.a;boost_random-mt.a")
if(APPLE)
    set(Boost_LIBRARIES_SHARED  "boost_thread-mt.dylib;boost_filesystem-mt.dylib;boost_system-mt.dylib;boost_random.dylib")
else()
    set(Boost_LIBRARIES_SHARED  "boost_thread-mt.so;boost_filesystem-mt.so;boost_system-mt.so;boost_random-mt.so")
endif()
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})

# Utility libs
find_library(LTDL_LIBRARY libltdl.a ltdl)
find_library(RT_LIBRARY rt)
find_library(ZLIB_LIBRARY libz.a z)
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
set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/${LIB_DIR}:${CMAKE_CURRENT_BINARY_DIR}:.:${LIB_DIR}:../${LIB_DIR}:./c_lib:../c_lib")

if(APPLE)
    set(PLATFORM_EXTRA_LIBS "${CMAKE_THREAD_LIBS_INIT};${LTDL_LIBRARY};${ZLIB_LIBRARY};${DL_LIBRARY}")
    set(SECTION_FRAGMENTATION_FLAGS "-Wno-deprecated-declarations")
else()
    set(PLATFORM_EXTRA_LIBS "${CMAKE_THREAD_LIBS_INIT};${LTDL_LIBRARY};${ZLIB_LIBRARY};${DL_LIBRARY};${RT_LIBRARY}")
    set(SECTION_FRAGMENTATION_FLAGS "-fdata-sections -ffunction-sections -Wl,--gc-sections -Wl,-rpath,${CUSTOM_RPATH}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SECTION_FRAGMENTATION_FLAGS} -fPIC -O2 ${FUSE_DEFINITIONS} -DFUSE_USE_VERSION=29")
set(CMAKE_SKIP_RPATH TRUE) # Something is wrong with CMake RPATH config, we need to use old-fashion way

# Define targets

add_library(veilprotocol ${PROTO_SRCS})
add_library(veilhelpersShared SHARED ${HELPER_SOURCES})
add_library(veilhelpers STATIC ${HELPER_SOURCES})

add_dependencies(veilhelpers glog veilprotocol websocketpp)
add_dependencies(veilhelpersShared glog veilprotocol websocketpp)

set_target_properties(veilhelpersShared PROPERTIES OUTPUT_NAME veilhelpers)

message(${WSPP_INCLUDE_DIR})

target_link_libraries(veilhelpers
    ${FUSE_LIBRARIES}
    ${Boost_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${GLOG_LIBRARIES}
    veilprotocol
    ${PLATFORM_EXTRA_LIBS}
)

target_link_libraries(veilhelpersShared
    ${FUSE_LIBRARIES}
    ${Boost_LIBRARIES_SHARED}
    ${GLOG_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${GLOG_LIBRARIES}
    veilprotocol
    ${PLATFORM_EXTRA_LIBS}
)

target_link_libraries(veilprotocol
    ${PROTOBUF_LIBRARY}
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
