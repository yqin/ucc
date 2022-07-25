#!/bin/bash
module purge
module load workshop.x86

# Start the DPU daemons
#
# Param Comma separated list of hosts on which to start DPU daemons
#
function dpu_start_daemons
{
    local dpulist="$1"
    local ns="$2"
    local daemondir="$3"
    local conf_file="$4"
    local offloadlibs="$5"
    local global_id=0

    if [ -z "$dpulist" -o -z "$daemondir" ]; then
        echo "Usage: $0 <list of dpus> <# of service processes per dpu > <daemon exec dir> <configuration file path> <offload lib location>"
        exit -2
    #else
    #    echo "    DEBUG: dpulist=$dpulist"
    #    echo "    DEBUG: ns=$ns"
    #    echo "    DEBUG: daemondir=$daemondir"
    #    echo "    DEBUG: conf_file=$conf_file"
    #    echo "    DEBUG: offloadlibs=$offloadlibs"
    fi

    # For each BF setup the environment and start it
    daemonexe="${daemondir}/bin/ucc_offload_dpu_daemon"
    daemonenv="UCX_NET_DEVICES=mlx5_0:1 \
        UCX_TLS=rc_x \
        UCX_ZCOPY_THRESH=0 \
        UCX_MEM_INVALIDATE=n \
        UCX_LOG_LEVEL=warn \
        UCX_HANDLE_ERRORS=bt,freeze \
        DPU_OFFLOAD_DBG_VERBOSE=0 \
        OFFLOAD_CONFIG_FILE_PATH=${conf_file} \
        DPU_OFFLOAD_LIST_DPUS=${dpulist} \
        DPU_OFFLOAD_SERVICE_PROCESSES_PER_DPU=${ns} \
        LD_LIBRARY_PATH=${offloadlibs}"

    for dpu in $(echo $dpulist |sed "s/,/ /g"); do
        for local_id in $(seq 0 $(($ns - 1))); do
            daemonlog="$HOME/daemonlog-${SLURM_JOBID}-${dpu}-${local_id}.out"
            ssh "$dpu" "${daemonenv} DPU_OFFLOAD_SERVICE_PROCESS_GLOBAL_ID=${global_id} DPU_OFFLOAD_SERVICE_PROCESS_LOCAL_ID=${local_id} nohup $daemonexe &> $daemonlog &"
            echo "Daemon ($daemonexe) start status: $?"
            global_id=$((global_id + 1))
        done
    done

    local time=5s
    echo "Wait $time for daemon wireup to complete"
    sleep $time
}

# Stop the DPU daemons
#
# param Comma separated list of hosts with DPU
#
function dpu_stop_daemons {
    local dpulist=$1

    # Kill all the daemons
    for dpu in $(echo $dpulist |sed "s/,/ /g"); do
        ssh $dpu "pkill -f ucc_offload_dpu_daemon; [ "\$?" == "0" ] && echo \"$dpu: Daemon stopped\""
    done
}


if [ $# -ne 5 ]; then
    echo "Usage: $0 <UCC installation location> <ring|offload> <# of host processes> <# of processes per host> <# of service processes per dpu>, e.g., $0 \$HOME/ucc.install offload 2 1 1"
    exit -1
fi

PREFIX="$1"
ALGO="${2:-ring}"
NP=${3:-2}
PPN=${4:-1}
NS=${5:-1}
HOSTLIST=$(scontrol show hostname ${SLURM_NODELIST} |grep -v 'bf' |sed -e "s/\$/:${SLURM_CPUS_ON_NODE}/g" |paste -d , -s)
DPULIST=$(scontrol show hostname ${SLURM_NODELIST} |grep 'bf' |sed -e "s/\$//g" |paste -d , -s)
CONFIG=${RDMA_DIR}/etc/thor_${NS}sp.cfg
OFFLOADLIBS=${RDMA_DIR}/dpu.arm/lib:${RDMA_DIR}/ucx.arm/lib:${RDMA_DIR}/ompi.arm/lib

echo "UCC location:           ${PREFIX}"
echo "Allgatherv algorithm:   ${ALGO}"
echo "HOST list:              ${HOSTLIST}, NP=${NP}, PPN=${PPN}"
echo "DPU list:               ${DPULIST}, NS=${NS}"
echo "Offload config file:    ${CONFIG}"

dpu_stop_daemons "${DPULIST}"
dpu_start_daemons "${DPULIST}" "${NS}" "${PREFIX}" "${CONFIG}" "${OFFLOADLIBS}"

export LD_LIBRARY_PATH=$PREFIX/lib:$LD_LIBRARY_PATH
mpirun  -np ${NP} \
        -H ${HOSTLIST} \
        --map-by ppr:${PPN}:node:oversubscribe \
        --bind-to core \
        --rank-by core \
        --mca pml ucx \
            -x UCX_NET_DEVICES=mlx5_2:1 \
            -x UCX_TLS=rc_x \
            -x UCX_MEM_INVALIDATE=n \
            -x UCX_LOG_LEVEL=warn \
            -x UCX_HANDLE_ERRORS=bt,freeze \
        --mca coll_ucc_enable 1 \
        --mca coll_ucc_priority 100 \
            -x UCC_TL_UCP_TUNE="allgatherv:256-inf:@${ALGO}" \
            -x UCC_LOG_LEVEL=warn \
        -x OFFLOAD_CONFIG_FILE_PATH=${CONFIG} \
        -x DPU_OFFLOAD_DBG_VERBOSE=0 \
        -x PMIX_MCA_psec=native \
        ${RDMA_DIR}/osu-micro-benchmarks/bin/osu_iallgatherv -f -m 256:

dpu_stop_daemons "${DPULIST}"
