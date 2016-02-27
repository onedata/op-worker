CXX ?= g++

LDFLAGS += $(shell pkg-config --libs botan-1.10) \
-Wl,-rpath -Wl,$$ORIGIN/helpers/lib -L ../priv/helpers/lib -lhelpers \
-lboost_filesystem -lrados -ls3 -lglog -shared

CXXFLAGS += -fPIC -std=c++14 -O3 -Wall $(shell pkg-config --cflags botan-1.10) \
-isystem ../priv/helpers/include -D_FILE_OFFSET_BITS=64 -DASIO_STANDALONE

all: \
		../priv/csr_creator_drv.so \
		../priv/helpers_nif.so \
		../priv/rt_priority_queue_drv.so \
		../priv/rt_map_drv.so

../priv/csr_creator_drv.so: \
		csr_creator/csr_creator.o \
		csr_creator/csr_creator_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

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
