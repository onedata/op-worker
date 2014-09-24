cmake_minimum_required(VERSION 2.8.4)
project(oneproxy_gsi)

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# Setup Globus
message(STATUS "Checking for Globus...")
find_package(Globus REQUIRED)
include_directories(${GLOBUS_INCLUDE_DIR})

set(GPV_SOURCE_FILES grid_proxy_verify.c grid_proxy_verify.h)
add_library(oneproxy_gsi STATIC ${GPV_SOURCE_FILES})

set(ONEPROXY_GSI_LIBRARIES ${OPENSSL_LIBRARIES} ${GLOBUS_LIBRARIES} PARENT_SCOPE)