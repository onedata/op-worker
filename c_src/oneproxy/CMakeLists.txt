cmake_minimum_required(VERSION 3.0.0)
project(oneproxy C CXX)

# Setup PkgConfig
message(STATUS "Checking for pkg-config...")
find_package(PkgConfig REQUIRED)

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup Boost
set(Boost_USE_STATIC_LIBS       OFF)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS filesystem system thread REQUIRED)

# Setup Globus
pkg_check_modules(Globus REQUIRED globus-common globus-gsi-callback)

# Common targets' setup
link_directories(${Globus_LIBRARY_DIRS})
add_compile_options(-Wall)

# GPV target settings
set(GPV_SOURCE_FILES
    src/gpv/grid_proxy_verify.c
    src/gpv/grid_proxy_verify.h)

add_library(oneproxy_gsi STATIC ${GPV_SOURCE_FILES})

target_link_libraries(oneproxy_gsi PUBLIC
    ${Globus_LIBRARIES}
    ${OPENSSL_LIBRARIES})

target_include_directories(oneproxy_gsi SYSTEM PUBLIC
    ${Globus_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR})

target_compile_options(oneproxy_gsi PUBLIC ${Globus_CFLAGS_OTHER})

# oneproxy main target settings
set(SOURCE_FILES
    src/main.cpp
    src/log_message.h
    src/tls_server.cpp
    src/tls2tcp_session.cpp
    src/base64.cpp)

add_executable(oneproxy ${SOURCE_FILES})

target_compile_options(oneproxy PRIVATE -std=c++11)

target_include_directories(oneproxy SYSTEM PRIVATE
    ${OPENSSL_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS})

set(CUSTOM_RPATH "\$ORIGIN" "\$ORIGIN/.." "." "..")
set_target_properties(oneproxy PROPERTIES
    BUILD_WITH_INSTALL_RPATH true
    INSTALL_RPATH_USE_LINK_PATH true
    INSTALL_RPATH "${CUSTOM_RPATH}")

target_link_libraries(oneproxy
    ${Boost_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    oneproxy_gsi
    ${CMAKE_THREAD_LIBS_INIT}
)

install(TARGETS oneproxy DESTINATION .)
