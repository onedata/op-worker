CXX ?= g++

HELPERS_DIR ?= ../_build/default/lib/helpers

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/include/\", [code:root_dir(), erlang:system_info(version)]).")

LDFLAGS += -shared -Wl,-rpath -Wl,\$$ORIGIN/helpers/lib -L $(HELPERS_DIR)/priv/lib \
	-lhelpers $(shell pkg-config --libs botan-1.10 tbb fuse) -laws-cpp-sdk-s3 \
	-laws-cpp-sdk-core -lcurl -lrados -lradosstriper -lSwift -lPocoUtil -lPocoXML \
	-lPocoNet -lPocoFoundation -lpthread -lfolly \
	-lglog -lnss3 -lnspr4 -lboost_system -lboost_filesystem -lboost_log -lboost_thread \
	-lboost_context -liberty -levent -ldouble-conversion -lboost_iostreams -lboost_regex \
	$(shell pkg-config --libs glusterfs-api)

CXXFLAGS += -fPIC -std=c++14 -O3 -Wall -D_FILE_OFFSET_BITS=64 -DASIO_STANDALONE \
    -DWITH_CEPH=1 -DWITH_S3=1 -DWITH_SWIFT=1 -DWITH_GLUSTERFS=1 \
    $(shell pkg-config --cflags botan-1.10 tbb fuse glusterfs-api) \
    -isystem $(HELPERS_DIR)/priv/include -isystem $(HELPERS_DIR)/deps/cppmetrics \
    -isystem $(ERTS_INCLUDE_DIR)

all: ../priv/helpers_nif.so

../priv/helpers_nif.so: helpers/helpers_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CFLAGS) -c $< -o $@
