cmake_minimum_required(VERSION 2.8.4)
project(oneproxy)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})

add_subdirectory(src/gpv)

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup botan
message(STATUS "Checking for botan...")
find_package(Botan REQUIRED)

# Setup Boost
set(Boost_USE_STATIC_LIBS       OFF)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS filesystem system thread REQUIRED)
include_directories(${Boost_INCLUDE_DIRS} ${BOTAN_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=c++11")

set(SOURCE_FILES src/main.cpp src/log_message.h src/tls_server.cpp src/tls2tcp_session.cpp src/base64.cpp)
add_executable(oneproxy ${SOURCE_FILES})

set(CUSTOM_RPATH "\$ORIGIN:\$ORIGIN/..:.:..")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${CUSTOM_RPATH}")

target_link_libraries(oneproxy
    ${Boost_LIBRARIES}
    ${Threads_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${BOTAN_LIBRARY}
    ${ONEPROXY_GSI_LIBRARIES}
    oneproxy_gsi
    ${CMAKE_THREAD_LIBS_INIT}
)

install(TARGETS oneproxy DESTINATION .)