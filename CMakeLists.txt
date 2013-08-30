cmake_minimum_required (VERSION 2.8.0)
project (VeilHelpers)

# The version number.
set(VeilHelpers_VERSION_MAJOR 0)
set(VeilHelpers_VERSION_MINOR 1)
set(VeilHelpers_VERSION_PATCH 0)
set(VeilHelpers_VERSION $(VeilHelpers_VERSION_MAJOR).$(VeilHelpers_VERSION_MINOR).$(VeilHelpers_VERSION_PATCH))


# CMake config
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})

# Include paths
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_BINARY_DIR})

# Setup fuse
message(STATUS "Checking for FUSE...")
find_package(FUSE REQUIRED)
include_directories(${FUSE_INCLUDE_DIRS})

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# Setup Boost
set(Boost_USE_STATIC_LIBS        ON)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS system thread filesystem REQUIRED)
set(Boost_LIBRARIES "boost_thread-mt.a;boost_filesystem-mt.a;boost_system-mt.a") # SL6 bug fix
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})

# Setup sources
file(GLOB HELPER_SOURCES "src/[a-zA-Z]*.cc")

# Setup compile flags
set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/${LIB_DIR}:${CMAKE_CURRENT_BINARY_DIR}:.:${LIB_DIR}:../${LIB_DIR}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 ${FUSE_DEFINITIONS} -Wl,-rpath,${CUSTOM_RPATH}")
set(CMAKE_SKIP_RPATH TRUE) # Something is wrong with CMake RPATH config, we need to use old-fashion way

# Define targets

add_library(veilhelpers ${HELPER_SOURCES})

target_link_libraries(veilhelpers
    ${FUSE_LIBRARIES}
    ${Boost_LIBRARIES}
    veilhelpers
    dl
    z
)