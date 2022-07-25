#define _POSIX_C_SOURCE 200809L

//
// Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
//
// See LICENSE.txt for license information
//

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>

#include "dpu_offload_service_daemon.h"
#include "dpu_offload_envvars.h"
#include "../allgatherv/allgatherv_offload_dpu.h"

int main(int argc, char **argv)
{
    /*
     * BOOTSTRAPPING: WE CREATE A CLIENT THAT CONNECT TO THE INITIATOR ON THE HOST
     * AND INITIALIZE THE OFFLOADING SERVICE.
     */
    ucs_info("Creating offload engine...");
    offloading_engine_t *offload_engine;
    dpu_offload_status_t rc = offload_engine_init(&offload_engine);
    if (rc || offload_engine == NULL)
    {
        ucs_error("offload_engine_init() failed");
        return EXIT_FAILURE;
    }
    ucs_info("Offload engine successfully created");

    /* register AM callbacks */
    ucs_info("Registering notification callbacks...");
    rc = register_allgatherv_dpu_notifications(offload_engine);
    if (rc) {
        ucs_error("failed to register event notification callbacks");
        return EXIT_FAILURE;
    }

    /*
     * GET THE CONFIGURATION.
     */
    ucs_info("Getting configuration...");
    offloading_config_t config_data;
    INIT_DPU_CONFIG_DATA(&config_data);
    config_data.offloading_engine = offload_engine;
    int ret = get_dpu_config(offload_engine, &config_data);
    if (ret)
    {
        ucs_error("get_config() failed");
        return EXIT_FAILURE;
    }
    ucs_info("Configuration successfully obtained");

    /*
     * INITIATE CONNECTION BETWEEN SERVICE PROCESSES ON DPUS.
     */
    ucs_info("Initiating connections between service processes running on DPUs...");
    rc = inter_dpus_connect_mgr(offload_engine, &config_data);
    if (rc)
    {
        ucs_error("inter_dpus_connect_mgr() failed");
        return EXIT_FAILURE;
    }
    ucs_info("Connections between service processes successfully initialized");
    while (offload_engine->num_service_procs != offload_engine->num_connected_service_procs + 1)
        offload_engine_progress(offload_engine);
    ucs_info("Now connected to all service processes");

    /*
     * CREATE A SERVER SO THAT PROCESSES RUNNING ON THE HOST CAN CONNECT.
     */
    ucs_info("Creating server for processes on the DPU...");
    // We let the system figure out the configuration to use to let ranks connect
    SET_DEFAULT_DPU_HOST_SERVER_CALLBACKS(&(config_data.local_service_proc.host_init_params));
    // The first slots in the array of servers are reserved for inter-DPUs servers
    // where the index is equal to the DPU ID.
    config_data.local_service_proc.host_init_params.id = offload_engine->num_service_procs;
    config_data.local_service_proc.host_init_params.id_set = true;
    execution_context_t *service_server = server_init(offload_engine, &(config_data.local_service_proc.host_init_params));
    if (service_server == NULL)
    {
        ucs_error("service_server is undefined");
        return EXIT_FAILURE;
    }
    ucs_info("Server for application processes to connect has been successfully created");

    /* initialize active collectives */
    ucs_list_head_init(&active_colls);

    /* initialize pending rts */
    ucs_list_head_init(&pending_rts);

    /* initialize pending receives */
    ucs_list_head_init(&pending_receives);

    /* initialize posted receives */
    ucs_list_head_init(&posted_receives);

    /*
     * PROGRESS UNTIL ALL PROCESSES ON THE HOST SEND A TERMINATION MESSAGE
     */
    ucs_info("%s: progressing...", argv[0]);
    while (!EXECUTION_CONTEXT_DONE(service_server))
    {
        lib_progress(service_server);
        progress_ops(service_server);
    }

    ucs_info("%s: server done, finalizing...", argv[0]);

    offload_engine_fini(&offload_engine);
    ucs_info("client all done, exiting successfully");

    return EXIT_SUCCESS;
}
