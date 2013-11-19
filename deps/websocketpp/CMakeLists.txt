include(ExternalProject)

set(WSPP_INSTALL_DIR "${PROJECT_BINARY_DIR}")
ExternalProject_Add(websocketpp
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    GIT_REPOSITORY https://github.com/zaphoyd/websocketpp
    CONFIGURE_COMMAND ${GLOG_CONFIGURE_COMMAND}
    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${WSPP_INSTALL_DIR}
)

# Export dep includes and lib paths
set(WSPP_INCLUDE_DIR ${WSPP_INSTALL_DIR}/include PARENT_SCOPE)
