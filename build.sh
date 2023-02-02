#!/bin/bash

if [[ -z ${SLURM_JOBID} ]]; then
    echo "Not in a Slurm allocation"
    exit -1
fi

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

# 1. enable dpu offload build
./configure --prefix="$PREFIX" --with-mpi --with-ucx="$RDMA_DIR/ucx.x86" --with-dpu-offload="$RDMA_DIR/dpu.x86" 2>&1 | tee configure.log

# 2. install to the preferred location
make -j install 2>&1 | tee make.log

# 3. build dpu daemon
ssh $(scontrol show hostname ${SLURM_NODELIST} | grep "bf" | head -1) "cd $PWD/src/components/tl/ucp/offload_dpu_daemon; source /usr/share/Modules/init/bash; module load workshop.arm; gcc *.c -g -ldpuoffloaddaemon -lucp -lucs -o $PREFIX/bin/ucc_offload_dpu_daemon.arm" 2>&1 | tee -a make.log

make -j distclean
