#!/bin/bash

if [ -z $1 ]; then
    echo "Usage: $0 <UCC installation location>, e.g., $0 \$HOME/ucc.install"
    exit -1
fi

module purge
module load workshop.x86
export PREFIX="$1"
rm -rf "$PREFIX"

make -j distclean

# 0. run autogen
./autogen.sh

# 1. uncomment below to enable dpu offload build
CFLAGS="-O0 -g" ./configure --prefix="$PREFIX" --enable-debug --with-mpi --with-ucx="$RDMA_DIR/ucx.x86" --with-dpu-offload="$RDMA_DIR/dpu.x86" 2>&1 | tee configure.log

# 2. install to the preferred location
make -j install 2>&1 | tee make.log

# 3. build dpu daemon
ssh $(scontrol show hostname ${SLURM_NODELIST} | grep "bf" | head -1) "sh $PWD/build.daemon.sh $PREFIX" 2>&1 | tee -a make.log

make -j distclean
