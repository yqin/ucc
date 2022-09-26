#!/bin/bash

if [ -z $1 ]; then
    echo "Usage: $0 <UCC installation location>, e.g., $0 \$HOME/ucc.install"
    exit -1
fi

module purge
module load workshop.arm
export PREFIX="$1"

cd `dirname $0`/src/components/tl/ucp/offload_dpu_daemon
gcc offload_dpu_daemon.c -g -ldpuoffloaddaemon -lucp -lucs -o ucc_offload_dpu_daemon
cp ucc_offload_dpu_daemon "$PREFIX/bin"
rm -f ucc_offload_dpu_daemon
