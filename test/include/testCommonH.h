/**
 * @file testCommonH.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef TEST_COMMON_H_H
#define TEST_COMMON_H_H

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "boost/bind.hpp" 
#include "glog/logging.h"

using namespace testing;
using namespace boost;
using namespace std;

namespace veil {
namespace helpers {
    // Just definition
}
}

using namespace veil; 
using namespace veil::helpers; 

#define INIT_AND_RUN_ALL_TESTS() \
    int main(int argc, char **argv) { \
        ::testing::InitGoogleTest(&argc, argv); \
        ::testing::InitGoogleMock(&argc, argv); \
        google::InitGoogleLogging(argv[0]); \
        FLAGS_alsologtostderr = false; \
        FLAGS_stderrthreshold = 3; \
        return RUN_ALL_TESTS(); \
    }

template<typename T> bool identityEqual( const T &lhs, const T &rhs ) { return &lhs == &rhs; }

#endif // TEST_COMMON_H_H
