#! /bin/bash

PATH=$PATH:.
#export LD_LIBRARY_PATH=../lib/nvml/src/nondebug/:../build:../../shim/glibc-build/rt/:../lib/libspdk/libspdk/
export LD_LIBRARY_PATH=../lib/nvml/src/nondebug/:../build:../../shim/glibc-build/rt:../../shim/glibc-build/:../lib/libspdk/libspdk/:/usr/lib/x86_64-linux-gnu/:/home/will/datacenter/strata/libfs/src/storage/spdk/

LD_PRELOAD=../../shim/libshim/libshim.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2 ${@}
#LD_PRELOAD=../../shim/libshim/libshim.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2 MLFS_PROFILE=1 ${@}
