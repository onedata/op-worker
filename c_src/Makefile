CXX ?= g++

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/include/\", [code:root_dir(), erlang:system_info(version)]).")

LDFLAGS += -shared -Wl,-rpath -Wl,\$$ORIGIN/helpers/lib -L ../priv/helpers/lib \
	-lhelpers $(shell pkg-config --libs botan-1.10 tbb fuse) -laws-cpp-sdk-s3 \
	-laws-cpp-sdk-core -lcurl -lrados -lradosstriper -lSwift -lPocoUtil -lPocoXML \
	-lPocoNet -lPocoFoundation -lpthread -Wl,-Bstatic -lfolly -Wl,-Bdynamic \
	-lglog -lnss3 -lnspr4 -lboost_filesystem -lboost_log -lboost_thread \
	-lboost_context -liberty -levent -ldouble-conversion -lboost_iostreams \
	$(shell pkg-config --libs glusterfs-api)

CXXFLAGS += -fPIC -std=c++14 -O3 -Wall -D_FILE_OFFSET_BITS=64 -DASIO_STANDALONE \
    -DWITH_CEPH=1 -DWITH_S3=1 -DWITH_SWIFT=1 -DWITH_GLUSTERFS=1 \
    $(shell pkg-config --cflags botan-1.10 tbb fuse glusterfs-api) \
	-isystem ../priv/helpers/include -isystem $(ERTS_INCLUDE_DIR)

all: \
		../priv/helpers_nif.so \
		../priv/rt_priority_queue_drv.so \
		../priv/rt_map_drv.so

../priv/helpers/lib/libhelpers.a:
	make LDFLAGS= \
         CFLAGS= \
         CXXFLAGS= \
         INSTALL_PREFIX=../../priv/helpers \
         BUILD_PROXY_IO=OFF \
         WITH_CEPH=ON \
         WITH_S3=ON \
         WITH_SWIFT=ON \
         WITH_GLUSTERFS=ON \
         -C ../helpers/ install
	rm -rf ../priv/helpers/**/*.so

%.o: %.cc ../priv/helpers/lib/libhelpers.a
	$(CXX) $(CXXFLAGS) $(CFLAGS) -c $< -o $@

../priv/helpers_nif.so: \
		helpers/helpers_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

../priv/rt_priority_queue_drv.so: \
		rtransfer/rt_priority_queue_nif.o \
		rtransfer/rt_interval.o \
		rtransfer/rt_block.o \
		rtransfer/rt_priority_queue.o
	$(CXX) $? -o $@ $(LDFLAGS)

../priv/rt_map_drv.so: \
		rtransfer/rt_map_nif.o \
		rtransfer/rt_block.o \
		rtransfer/rt_map.o
	$(CXX) $? -o $@ $(LDFLAGS)
