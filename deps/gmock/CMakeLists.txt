project(dep-gmock)

include(ExternalProject)

find_library(GMOCK_FIND_LIBRARY libgmock.a)
find_library(GTEST_FIND_LIBRARY libgtest.a)
find_library(GMOCK_MAIN_FIND_LIBRARY libgmock_main.a)
find_library(GTEST_MAIN_FIND_LIBRARY libgtest_main.a)
if(${GMOCK_FIND_LIBRARY} MATCHES GMOCK_FIND_LIBRARY-NOTFOUND OR ${GTEST_FIND_LIBRARY} MATCHES GTEST_FIND_LIBRARY-NOTFOUND
   OR ${GMOCK_MAIN_FIND_LIBRARY} MATCHES GMOCK_MAIN_FIND_LIBRARY-NOTFOUND OR ${GTEST_MAIN_FIND_LIBRARY} MATCHES GTEST_MAIN_FIND_LIBRARY-NOTFOUND)

    ExternalProject_Add(gmock
        PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
        SVN_REPOSITORY http://googlemock.googlecode.com/svn/trunk/
        SVN_REVISION -r 476
        INSTALL_COMMAND ""
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                   -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
                   -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
                   -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
                   --no-warn-unused-cli
    )

    # Export dep includes and lib paths
    ExternalProject_Get_Property(gmock binary_dir)
    ExternalProject_Get_Property(gmock source_dir)
    set(GMOCK_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)
    set(GMOCK_LIBRARIES ${binary_dir}/libgmock.a  ${binary_dir}/libgmock_main.a PARENT_SCOPE)

    set(GTEST_INCLUDE_DIR ${source_dir}/gtest/include PARENT_SCOPE)
    set(GTEST_LIBRARIES ${binary_dir}/gtest/libgtest.a  ${binary_dir}/gtest/libgtest_main.a PARENT_SCOPE)

else()

    get_filename_component(GMOCK_TMP_PATH ${GMOCK_FIND_LIBRARY} PATH)
    get_filename_component(GTEST_TMP_PATH ${GTEST_FIND_LIBRARY} PATH)

    set(GTEST_INCLUDE_DIR "${GTEST_TMP_PATH}/../include" PARENT_SCOPE)
    set(GTEST_LIBRARIES ${GTEST_FIND_LIBRARY} ${GTEST_MAIN_FIND_LIBRARY} PARENT_SCOPE)

    set(GMOCK_INCLUDE_DIR "${GMOCK_TMP_PATH}/../include" PARENT_SCOPE)
    set(GMOCK_LIBRARIES ${GMOCK_FIND_LIBRARY} ${GMOCK_MAIN_FIND_LIBRARY} PARENT_SCOPE)

endif()
