CXX ?= g++

HELPERS_DIR ?= ../_build/default/lib/helpers

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/include/\", [code:root_dir(), erlang:system_info(version)]).")

ifndef ONEDATA_SCL
	LDFLAGS += -shared -Wl,-rpath -Wl,\$$ORIGIN/helpers/lib -L $(HELPERS_DIR)/priv/lib \
	-lhelpers $(shell pkg-config --libs botan-2 tbb fuse3) -laws-cpp-sdk-s3 \
	-laws-cpp-sdk-core -laws-c-event-stream -laws-checksums -laws-c-common -lcurl \
	-lrados -lradosstriper -lSwift -lPocoUtil -lPocoXML \
	-lPocoNet -lPocoFoundation -lpthread -lfolly -Wl,--whole-archive -lfizz -lproxygen -lwangle -lsodium \
	-Wl,--no-whole-archive -lz -lglog -lnss3 -lnspr4 -lboost_system -lboost_filesystem -lboost_log -lboost_thread \
	-lboost_context -liberty -levent -ldouble-conversion -lboost_iostreams -lboost_regex \
	-lacl -lgfapi -lglusterfs -lgfrpc -lgfxdr -lXrdCl -lXrdPosixPreload -lXrdXml \
	-lXrdUtils -lXrdPosix -lspdlog -lfmt -lnfs
else
	LDFLAGS += -shared -Wl,-rpath -Wl,\$$ORIGIN/helpers/lib -L $(HELPERS_DIR)/priv/lib \
	-lhelpers $(shell pkg-config --libs botan tbb fuse3) -laws-cpp-sdk-s3 \
	-laws-cpp-sdk-core -laws-c-event-stream -laws-checksums -laws-c-common -lcurl \
	-lrados -lradosstriper -lSwift -lPocoUtil -lPocoXML \
	-lPocoNet -lPocoFoundation -lpthread -lfolly -Wl,--whole-archive -lfizz -lproxygen -lwangle -lsodium \
	-Wl,--no-whole-archive -lz -lglog -lnss3 -lnspr4 -lboost_system -lboost_filesystem -lboost_log -lboost_thread \
	-lboost_context -liberty -levent -ldouble-conversion -lboost_iostreams -lboost_regex \
	-lacl -lgfapi -lglusterfs -lgfrpc -lgfxdr -lXrdCl -lXrdPosixPreload -lXrdXml \
	-lXrdUtils -lXrdPosix -lspdlog -lfmt -lnfs
endif

# Add ceph-common library, if X_SCLS is defined, the library is installed
# as Software Collection on CentOS
ifndef ONEDATA_SCL
	LDFLAGS += -Wl,-rpath,/usr/lib/x86_64-linux-gnu/ceph/ /usr/lib/x86_64-linux-gnu/ceph/libceph-common.so.2
else
	LDFLAGS += -Wl,-rpath,/opt/onedata/$(ONEDATA_SCL)/root/usr/lib64/ceph/ /opt/onedata/$(ONEDATA_SCL)/root/usr/lib64/ceph/libceph-common.so.0
endif

ifndef ONEDATA_SCL
	CXXFLAGS += -fPIC -std=c++14 -O3 -Wall -D_FILE_OFFSET_BITS=64 -DASIO_STANDALONE \
    -DWITH_CEPH=1 -DWITH_S3=1 -DWITH_SWIFT=1 -DWITH_GLUSTERFS=1 -DWITH_WEBDAV=1 \
    -DWITH_XROOTD=1 -DWITH_NFS=1 \
    -DSPDLOG_COMPILED_LIB -DSPDLOG_FMT_EXTERNAL \
    $(shell pkg-config --cflags botan-2 tbb fuse3) \
    -isystem $(HELPERS_DIR)/priv/include -isystem $(HELPERS_DIR)/deps/cppmetrics \
    -isystem $(HELPERS_DIR)/deps/spdlog -isystem $(ERTS_INCLUDE_DIR)
else
	CXXFLAGS += -fPIC -std=c++14 -O3 -Wall -D_FILE_OFFSET_BITS=64 -DASIO_STANDALONE \
    -DWITH_CEPH=1 -DWITH_S3=1 -DWITH_SWIFT=1 -DWITH_GLUSTERFS=1 -DWITH_WEBDAV=1 \
    -DWITH_XROOTD=1 -DWITH_NFS=1 \
    -DSPDLOG_COMPILED_LIB -DSPDLOG_FMT_EXTERNAL \
    $(shell pkg-config --cflags botan tbb fuse3) \
    -isystem $(HELPERS_DIR)/priv/include -isystem $(HELPERS_DIR)/deps/cppmetrics \
    -isystem $(HELPERS_DIR)/deps/spdlog -isystem $(ERTS_INCLUDE_DIR)
endif

all: ../priv/helpers_nif.so

../priv/helpers_nif.so: helpers/helpers_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CFLAGS) -c $< -o $@
