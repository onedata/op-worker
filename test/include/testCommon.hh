/**
 * @file testCommon.hh
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "boost/bind.hpp" 

using namespace testing;
using namespace boost;


#define INIT_AND_RUN_ALL_TESTS() \
    int main(int argc, char **argv) { \
        ::testing::InitGoogleTest(&argc, argv); \
        ::testing::InitGoogleMock(&argc, argv); \
        return RUN_ALL_TESTS(); \
    }

template<typename T> bool identityEqual( const T &lhs, const T &rhs ) { return &lhs == &rhs; }

#endif // TEST_COMMON_H
