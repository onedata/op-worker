include(ExternalProject)

ExternalProject_Add(websocketpp
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    GIT_REPOSITORY https://github.com/RoXeon/websocketpp
    UPDATE_COMMAND ""
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_CXX_FLAGS=-fPIC -std=c++11 -w
)

ExternalProject_Get_Property(websocketpp install_dir)

# Export dep includes and lib paths
set(WSPP_INCLUDE_DIR ${install_dir}/include PARENT_SCOPE)
