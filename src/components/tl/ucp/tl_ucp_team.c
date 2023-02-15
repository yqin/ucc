/**
 * Copyright (C) Mellanox Technologies Ltd. 2020-2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "tl_ucp.h"
#include "tl_ucp_ep.h"
#include "tl_ucp_coll.h"
#include "tl_ucp_sendrecv.h"
#include "utils/ucc_malloc.h"
#include "coll_score/ucc_coll_score.h"

#ifdef HAVE_DPU_OFFLOAD
#include "dpu_offload_service_daemon.h"
#include "dpu_offload_envvars.h"
#include "allgatherv/allgatherv_offload_host.h"

ucc_status_t ucc_tl_ucp_team_offload_engine_init(ucc_tl_ucp_team_t *team, const ucc_base_team_params_t *params);
ucc_status_t ucc_tl_ucp_team_offload_engine_fini(ucc_tl_ucp_team_t *team);

// Once and only one engine
struct {
    char *cfg_file;
    offloading_config_t dpus_config;
    offloading_engine_t *engine;
    int                 engine_ref_count;
} ucc_tl_ucp_offloading;
#endif

UCC_CLASS_INIT_FUNC(ucc_tl_ucp_team_t, ucc_base_context_t *tl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_tl_ucp_context_t *ctx =
        ucc_derived_of(tl_context, ucc_tl_ucp_context_t);

    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);
    /* TODO: init based on ctx settings and on params: need to check
             if all the necessary ranks mappings are provided */
    self->preconnect_task    = NULL;
    self->seq_num            = 0;
    self->status             = UCC_INPROGRESS;
#ifdef HAVE_DPU_OFFLOAD
    tl_debug(tl_context->lib, "Initializing team's offloading data...\n");
    self->dpu_offloading_econtext = NULL;
    ucc_tl_ucp_team_offload_engine_init(self, params);
#endif // HAVE_DPU_OFFLOAD

    tl_info(tl_context->lib, "posted tl team: %p", self);
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_ucp_team_t)
{
    tl_info(self->super.super.context->lib, "finalizing tl team: %p", self);
#ifdef HAVE_DPU_OFFLOAD
    ucc_tl_ucp_offloading.engine_ref_count--;
    if (ucc_tl_ucp_offloading.engine_ref_count == 0) {
        tl_info(self->super.super.context->lib, "finalizing offload engine: %p", ucc_tl_ucp_offloading.engine);
        // Terminate the execution context
        ucc_tl_ucp_team_offload_engine_fini(self);
        self->dpu_offloading_econtext = NULL;
    }
#endif // HAVE_DPU_OFFLOAD
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_ucp_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_tl_ucp_team_t, ucc_tl_team_t);

ucc_status_t ucc_tl_ucp_team_destroy(ucc_base_team_t *tl_team)
{
    ucc_tl_ucp_team_t *team = ucc_derived_of(tl_team, ucc_tl_ucp_team_t);
    uint16_t team_id = team->super.super.params.id;
#ifdef HAVE_DPU_OFFLOAD
    if (!IS_SERVICE_TEAM(team) && team->dpu_offloading_econtext != NULL)
    {
#if !NDEBUG
        dpu_offload_status_t rc;
        ucc_status_t status = UCC_OK;
#endif // !NDEBUG
        rank_info_t rank_info;
        ucc_rank_t lead_rank = 0;
        ucc_sbgp_t *node_sbgp = NULL;
        ucc_rank_t local_rank = 0;
        size_t n_local_ranks = 0;
        ucc_topo_t *cur_topo = NULL;
        ucc_subset_t set;
        ucc_tl_ucp_context_t *ctx = NULL;
        ucc_context_t *core_ctx = NULL;
        bool core_topo_needs_to_be_freed = false;

        team->offloading_uid = INT_MAX;
        ctx = UCC_TL_UCP_TEAM_CTX(team);
        assert(ctx);
        core_ctx = ctx->super.super.ucc_context;
        assert(core_ctx);
        if (!core_ctx->topo) {
            ucc_context_topo_init(&core_ctx->addr_storage, &core_ctx->topo);
            core_topo_needs_to_be_freed = true;
        }
#if !NDEBUG
        status = ucc_ep_map_create_nested(&UCC_TL_CORE_TEAM(team)->ctx_map,
                                          &UCC_TL_TEAM_MAP(team), &team->ctx_map);
        assert (UCC_OK == status);
#else
        ucc_ep_map_create_nested(&UCC_TL_CORE_TEAM(team)->ctx_map,
                                 &UCC_TL_TEAM_MAP(team), &team->ctx_map);
#endif // !NDEBUG
        set.map = team->ctx_map;
        set.myrank = UCC_TL_TEAM_RANK(team);
        ucc_topo_init(set, core_ctx->topo, &cur_topo);
        assert(cur_topo);
        node_sbgp = ucc_topo_get_sbgp(cur_topo, UCC_SBGP_NODE);
        assert(node_sbgp);
        local_rank = node_sbgp->group_rank;
        n_local_ranks = node_sbgp->group_size;
        // Generate a unique group ID
        if (UCC_TL_CORE_TEAM(team) != NULL)
        {
            size_t idx;
            ucc_rank_t group_map[UCC_TL_TEAM_SIZE(team) + 2];
            lead_rank = ucc_ep_map_eval(UCC_TL_CORE_TEAM(team)->ctx_map, 0);
            group_map[0] = lead_rank;
            group_map[1] = team_id;
            for (idx = 0; idx < UCC_TL_TEAM_SIZE(team); idx++)
            {
                group_map[idx + 2] = ucc_ep_map_eval(UCC_TL_CORE_TEAM(team)->ctx_map, idx);
            }
            team->offloading_uid = HASH_GROUP(&group_map[0], UCC_TL_TEAM_SIZE(team) + 2);
        }
        rank_info.group_rank = UCC_TL_TEAM_RANK(team);
        rank_info.group_size = UCC_TL_TEAM_SIZE(team);
        rank_info.n_local_ranks = n_local_ranks;
        rank_info.local_rank = local_rank;
        assert(team->offloading_uid != INT_MAX);
        rank_info.group_uid = team->offloading_uid;
        ucc_topo_cleanup(cur_topo);
        cur_topo = NULL;
        if (core_topo_needs_to_be_freed)
        {
            ucc_context_topo_cleanup(core_ctx->topo);
            core_ctx->topo = NULL;
        }

        tl_debug(UCC_TL_TEAM_LIB(team), "Revoking group %d-%d 0x%x (pid:%d, rank: %d)...",
                 team_id, lead_rank, team->offloading_uid, getpid(), UCC_TL_TEAM_RANK(team));
#if !NDEBUG
        rc = send_revoke_group_rank_request_through_rank_info(team->dpu_offloading_econtext,
                                                              GET_SERVER_EP(team->dpu_offloading_econtext),
                                                              team->dpu_offloading_econtext->client->server_id,
                                                              &rank_info,
                                                              NULL);
        assert(rc == DO_SUCCESS);
#else
        send_revoke_group_rank_request_through_rank_info(team->dpu_offloading_econtext,
                                                         GET_SERVER_EP(team->dpu_offloading_econtext),
                                                         team->dpu_offloading_econtext->client->server_id,
                                                         &rank_info,
                                                         NULL);
#endif // !NDEBUG
    }
#endif // HAVE_DPU_OFFLOAD

    UCC_CLASS_DELETE_FUNC_NAME(ucc_tl_ucp_team_t)(tl_team);
    return UCC_OK;
}

static ucc_status_t ucc_tl_ucp_team_preconnect(ucc_tl_ucp_team_t *team)
{
    ucc_rank_t src, dst, size, rank;
    ucc_status_t status;
    int i;

    size = UCC_TL_TEAM_SIZE(team);
    rank = UCC_TL_TEAM_RANK(team);
    if (!team->preconnect_task) {
        team->preconnect_task = ucc_tl_ucp_get_task(team);
        team->preconnect_task->tag = 0;
    }
    if (UCC_INPROGRESS == ucc_tl_ucp_test(team->preconnect_task)) {
        return UCC_INPROGRESS;
    }
    for (i = team->preconnect_task->send_posted; i < size; i++) {
        src = (rank - i + size) % size;
        dst = (rank + i) % size;
        status = ucc_tl_ucp_send_nb(NULL, 0, UCC_MEMORY_TYPE_UNKNOWN, src, team,
                                    team->preconnect_task);
        if (UCC_OK != status) {
            return status;
        }
        status = ucc_tl_ucp_recv_nb(NULL, 0, UCC_MEMORY_TYPE_UNKNOWN, dst, team,
                                    team->preconnect_task);
        if (UCC_OK != status) {
            return status;
        }
        if (UCC_INPROGRESS == ucc_tl_ucp_test(team->preconnect_task)) {
            return UCC_INPROGRESS;
        }
    }
    tl_debug(UCC_TL_TEAM_LIB(team), "preconnected tl team: %p, num_eps %d",
             team, size);
    ucc_tl_ucp_put_task(team->preconnect_task);
    team->preconnect_task = NULL;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_ucp_team_t    *team = ucc_derived_of(tl_team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_context_t *ctx  = UCC_TL_UCP_TEAM_CTX(team);
    ucc_status_t          status;

    if (team->status == UCC_OK) {
        return UCC_OK;
    }
    if (UCC_TL_TEAM_SIZE(team) <= ctx->cfg.preconnect) {
        status = ucc_tl_ucp_team_preconnect(team);
        if (UCC_INPROGRESS == status) {
            return UCC_INPROGRESS;
        } else if (UCC_OK != status) {
            goto err_preconnect;
        }
    }

    if (ctx->remote_info) {
        for (int i = 0; i < ctx->n_rinfo_segs; i++) {
            team->va_base[i]     = ctx->remote_info[i].va_base;
            team->base_length[i] = ctx->remote_info[i].len;
        }
    }

#ifdef HAVE_DPU_OFFLOAD
    if (!IS_SERVICE_TEAM(team) && team->dpu_offloading_econtext != NULL)
    {
        offloading_engine_t *offloading_engine = ucc_tl_ucp_offloading.engine;
        offload_engine_progress(offloading_engine);
        if (!group_cache_populated(offloading_engine, team->offloading_uid))
            return UCC_INPROGRESS;
        tl_debug(UCC_TL_TEAM_LIB(team), "Rank %d: team successfully created (0x%x %p size: %d)",
                 UCC_TL_TEAM_RANK(team), team->offloading_uid, team, UCC_TL_TEAM_SIZE(team));

        /* register AM callbacks for allgathter to the client */
        status = register_allgatherv_host_notifications(team->dpu_offloading_econtext->event_channels);
        if (status) {
            tl_error(tl_team->context->lib, "Register event notification callbacks failed.");
            return status;
        } else {
            tl_debug(tl_team->context->lib, "Register event notification callbacks succeeded.");
        }
    }
#endif

    tl_info(tl_team->context->lib, "initialized tl team: %p", team);
    team->status = UCC_OK;
    return UCC_OK;

err_preconnect:
    return status;
}

#ifdef HAVE_DPU_OFFLOAD
ucc_status_t ucc_tl_ucp_team_offload_engine_fini(ucc_tl_ucp_team_t *team)
{
    if (IS_SERVICE_TEAM(team))
    {
        return UCC_OK;
    }

    if (ucc_tl_ucp_offloading.engine != NULL) {
        if (ucc_tl_ucp_offloading.engine->client != NULL)
            client_fini(&(ucc_tl_ucp_offloading.engine->client));
        offload_engine_fini(&(ucc_tl_ucp_offloading.engine));
    }
    return UCC_OK;
}

void create_team_map(ucc_tl_ucp_team_t *team, void *buff)
{
    int64_t i;
    if (UCC_TL_CORE_TEAM(team) == NULL)
        return;
    int64_t *ptr = (int64_t*)buff;
    for (i = 0; i < UCC_TL_TEAM_SIZE(team); i++)
    {
        ucc_rank_t world_comm_rank = ucc_ep_map_eval(UCC_TL_CORE_TEAM(team)->ctx_map, i);
        *ptr = world_comm_rank;
        ptr = (int64_t*)((ptrdiff_t)ptr + sizeof(int64_t));
    }
}

size_t get_team_packed_info_size(ucc_tl_ucp_team_t *team)
{
    size_t size_map_single_rank = sizeof(int64_t);
    return sizeof(rank_info_t) + (UCC_TL_TEAM_SIZE(team) * size_map_single_rank);
}

ucc_status_t ucc_tl_ucp_team_offload_engine_init(ucc_tl_ucp_team_t *team, const ucc_base_team_params_t *params)
{
    ucc_tl_ucp_lib_t     *lib      = UCC_TL_UCP_TEAM_LIB(team);
    ucc_tl_ucp_context_t *ctx      = UCC_TL_UCP_TEAM_CTX(team);
    ucc_status_t          status   = UCC_OK;
    dpu_offload_status_t  rc;

    if (IS_SERVICE_TEAM(team))
    {
        team->dpu_offloading_econtext = NULL;
        return UCC_OK;
    }

    /* Figure out the number of local ranks so we can notify the DPU and
       optimize the management of the EP cache. */
    ucc_subset_t set;
    ucc_topo_t *cur_topo;
    ucc_context_t *core_ctx = ctx->super.super.ucc_context;
    size_t n_local_ranks = 0;
    bool core_topo_needs_to_be_freed = false;
    if (!core_ctx->topo) {
        ucc_context_topo_init(&core_ctx->addr_storage, &core_ctx->topo);
        core_topo_needs_to_be_freed = true;
    }
    status = ucc_ep_map_create_nested(&UCC_TL_CORE_TEAM(team)->ctx_map,
                                      &UCC_TL_TEAM_MAP(team), &team->ctx_map);
    if (UCC_OK != status) {
        tl_error(lib, "failed to create create ctx map");
        return status;
    }
    // This is the set parameters that seem to work in this context, change carefully
    ucc_rank_t my_rank = UCC_TL_TEAM_RANK(team);
    set.map = team->ctx_map;
    set.myrank = my_rank;
    ucc_topo_init(set, core_ctx->topo, &cur_topo);
    assert(cur_topo);
    ucc_sbgp_t *node_sbgp = ucc_topo_get_sbgp(cur_topo, UCC_SBGP_NODE);
    n_local_ranks = node_sbgp->group_size;
    ucc_rank_t local_rank = node_sbgp->group_rank;
    ucc_topo_cleanup(cur_topo);
    cur_topo = NULL;

    if (core_topo_needs_to_be_freed)
    {
        ucc_context_topo_cleanup(core_ctx->topo);
        core_ctx->topo = NULL;
    }

    /* Figure out a unique team ID so we can correctly support non-overlapping
    communicators/teams. We also create a temp map of the communicator so we
    generate a unique ID. Having that ID gives us a unique way to identify a
    communicator even when communicators IDs are reused when communicators
    are being created and destroyed. */
    ucc_rank_t lead_rank = 0;
    team->offloading_uid = INT_MAX;
    if (UCC_TL_CORE_TEAM(team) != NULL)
    {
        size_t idx;
        ucc_rank_t group_map[UCC_TL_TEAM_SIZE(team) + 2];
        uint16_t team_id = team->super.super.params.id;
        lead_rank = ucc_ep_map_eval(UCC_TL_CORE_TEAM(team)->ctx_map, 0);
        group_map[0] = lead_rank;
        group_map[1] = team_id;
        for (idx = 0; idx < UCC_TL_TEAM_SIZE(team); idx++)
        {
            group_map[idx + 2] = ucc_ep_map_eval(UCC_TL_CORE_TEAM(team)->ctx_map, idx);
        }
        team->offloading_uid = HASH_GROUP(&group_map[0], UCC_TL_TEAM_SIZE(team) + 2);
    }

    // DPU offloading: during the initialization of the team, we check if
    // offloading is already initialized, i.e., is the engine is initialized
    // and then if the client to connect to the DPU is initialized. If not,
    // make sure it happens
    tl_debug(lib, "looking up offloading engine...");
    if (ucc_tl_ucp_offloading.engine == NULL)
    {
        rc = offload_engine_init(&(ucc_tl_ucp_offloading.engine));
        if (rc)
        {
            ucc_error("offload_engine_init() failed");
            return UCC_ERR_NO_MESSAGE;
        }
        tl_info(lib, "creating offload engine: %p", ucc_tl_ucp_offloading.engine);
        char *offloading_config_file = getenv(OFFLOAD_CONFIG_FILE_PATH_ENVVAR);
        if (offloading_config_file != NULL)
        {
            tl_debug(lib, "offloading configuration file defined");
            ucc_tl_ucp_offloading.cfg_file = offloading_config_file;
        }
        else
        {
            tl_debug(lib, "offloading configuration file undefined");
            ucc_tl_ucp_offloading.cfg_file = NULL;
        }
        INIT_DPU_CONFIG_DATA(&(ucc_tl_ucp_offloading.dpus_config));
        ucc_tl_ucp_offloading.engine->config = &(ucc_tl_ucp_offloading.dpus_config);
        ucc_tl_ucp_offloading.dpus_config.offloading_engine = ucc_tl_ucp_offloading.engine;
        rc = get_host_config(&(ucc_tl_ucp_offloading.dpus_config));
        if (rc)
        {
            ucc_error("get_host_config() failed");
            return UCC_ERR_NO_MESSAGE;
        }

        ucc_tl_ucp_offloading.engine_ref_count = 0;
    }

    assert(ucc_tl_ucp_offloading.engine);
    tl_debug(lib, "offloading engine %p initialized", ucc_tl_ucp_offloading.engine);
    if (ucc_tl_ucp_offloading.engine->client == NULL)
    {
        execution_context_t *econtext;
        init_params_t offloading_init_params;
        RESET_INIT_PARAMS(&offloading_init_params);
        conn_params_t client_conn_params;
        RESET_CONN_PARAMS(&client_conn_params);
        rank_info_t rank_info;
        if (ucc_tl_ucp_offloading.cfg_file != NULL)
        {
            tl_debug(lib, "Platform configuration file exists, loading...");
            rank_info.group_uid = team->offloading_uid;
            rank_info.group_rank = UCC_TL_TEAM_RANK(team);
            rank_info.group_size = UCC_TL_TEAM_SIZE(team);
            rank_info.n_local_ranks = n_local_ranks;
            rank_info.local_rank = local_rank;

            offloading_init_params.conn_params = &client_conn_params;
            offloading_init_params.worker      = ctx->ucp_worker;
            offloading_init_params.proc_info   = &rank_info;
            offloading_init_params.ucp_context = ctx->ucp_context;
            rc = get_local_service_proc_connect_info(&ucc_tl_ucp_offloading.dpus_config, &rank_info, &offloading_init_params);
            if (rc)
            {
                ucc_error("get_local_service_proc_connect_info() failed");
                return UCC_ERR_NO_MESSAGE;
            }

            tl_debug(lib, "DPU offloading: client initialization based on configuration file...");
            econtext = client_init(ucc_tl_ucp_offloading.engine, &offloading_init_params);
            assert(econtext);
            ADD_CLIENT_TO_ENGINE(econtext, ucc_tl_ucp_offloading.engine);
        }
        else
        {
            tl_debug(lib, "DPU offloading: client initialization without initialization parameters...");
            econtext = client_init(ucc_tl_ucp_offloading.engine, NULL);
            assert(econtext);
            ADD_CLIENT_TO_ENGINE(econtext, ucc_tl_ucp_offloading.engine);
        }
        team->dpu_offloading_econtext = econtext;
        ucc_tl_ucp_offloading.engine->client = econtext;
        tl_info(lib, "using offload engine %p for team %p", ucc_tl_ucp_offloading.engine, team);
        ucc_tl_ucp_offloading.engine_ref_count++;
        tl_debug(lib, "DPU offloading: client successfully initialized as %p (team: %p)\n", econtext, team);
    }
    else
    {
        // We already have a connection to the local DPU, we just need to notify the DPU of a new team
        tl_info(lib, "using offload engine %p for team %p", ucc_tl_ucp_offloading.engine, team);
        team->dpu_offloading_econtext = ucc_tl_ucp_offloading.engine->client;
        dpu_offload_event_t *ev;
        dpu_offload_event_info_t ev_info;
        rank_info_t *rank_info;
        RESET_EVENT_INFO(&ev_info);
        ev_info.payload_size = get_team_packed_info_size(team);
        rc = event_get(team->dpu_offloading_econtext->event_channels, &ev_info, &ev);
        if (rc != DO_SUCCESS)
        {
            ucc_error("event_get() failed");
            abort();
        }
        rank_info = (rank_info_t*)ev->payload;
        RESET_RANK_INFO(rank_info);
        rank_info->group_uid = team->offloading_uid;
        rank_info->group_rank = UCC_TL_TEAM_RANK(team);
        rank_info->group_size = UCC_TL_TEAM_SIZE(team);
        rank_info->n_local_ranks = n_local_ranks;
        rank_info->local_rank = local_rank;
        void *ptr = (void*)((ptrdiff_t)(ev->payload) + sizeof(rank_info_t));
        create_team_map(team, ptr);
        assert(team);
        assert(team->dpu_offloading_econtext);
        assert(GET_SERVER_EP(team->dpu_offloading_econtext));
        assert(team->dpu_offloading_econtext->client->server_id);
        tl_debug(lib, "Rank %d: initiating team 0x%x, id: %d, lead: %d, size: %d, n_local_ranks: %ld",
                 UCC_TL_TEAM_RANK(team), rank_info->group_uid, team->super.super.params.id, lead_rank, UCC_TL_TEAM_SIZE(team), n_local_ranks);
        int ret = send_add_group_rank_request(team->dpu_offloading_econtext,
                                              GET_SERVER_EP(team->dpu_offloading_econtext),
                                              team->dpu_offloading_econtext->client->server_id,
                                              ev);
        if (ret!= EVENT_DONE && ret != EVENT_INPROGRESS)
        {
            tl_debug(lib, "get_event() failed");
            return UCC_ERR_NO_MESSAGE;
        }

        ucc_tl_ucp_offloading.engine_ref_count++;
    }

    return status;
}
#endif // HAVE_DPU_OFFLOAD

ucc_status_t ucc_tl_ucp_team_get_scores(ucc_base_team_t   *tl_team,
                                        ucc_coll_score_t **score_p)
{
    ucc_tl_ucp_team_t          *team    = ucc_derived_of(tl_team,
                                                      ucc_tl_ucp_team_t);
    ucc_component_framework_t  *plugins = &ucc_tl_ucp.super.coll_plugins;
    ucc_tl_ucp_context_t       *tl_ctx  = UCC_TL_UCP_TEAM_CTX(team);
    ucc_base_context_t         *ctx     = UCC_TL_TEAM_CTX(team);
    int                         mt_n    = 0;
    ucc_memory_type_t           mem_types[UCC_MEMORY_TYPE_LAST];
    ucc_coll_score_t           *score, *tlcp_score;
    ucc_tl_coll_plugin_iface_t *tlcp;
    ucc_status_t                status;
    unsigned                    i;

    for (i = 0; i < UCC_MEMORY_TYPE_LAST; i++) {
        if (tl_ctx->ucp_memory_types & UCC_BIT(ucc_memtype_to_ucs[i])) {
            tl_debug(tl_team->context->lib,
                     "enable support for memory type %s",
                     ucc_memory_type_names[i]);
            mem_types[mt_n++] = (ucc_memory_type_t)i;
        }
    }

    /* There can be a different logic for different coll_type/mem_type.
       Right now just init everything the same way. */
    status = ucc_coll_score_build_default(tl_team, UCC_TL_UCP_DEFAULT_SCORE,
                              ucc_tl_ucp_coll_init, UCC_TL_UCP_SUPPORTED_COLLS,
                              mem_types, mt_n, &score);
    if (UCC_OK != status) {
        return status;
    }
    for (i = 0; i < UCC_TL_UCP_N_DEFAULT_ALG_SELECT_STR; i++) {
        status = ucc_coll_score_update_from_str(
            ucc_tl_ucp_default_alg_select_str[i], score, UCC_TL_TEAM_SIZE(team),
            ucc_tl_ucp_coll_init, &team->super.super, UCC_TL_UCP_DEFAULT_SCORE,
            ucc_tl_ucp_alg_id_to_init);
        if (UCC_OK != status) {
            tl_error(tl_team->context->lib,
                     "failed to apply default coll select setting: %s",
                     ucc_tl_ucp_default_alg_select_str[i]);
            goto err;
        }
    }
    if (strlen(ctx->score_str) > 0) {
        status = ucc_coll_score_update_from_str(
            ctx->score_str, score, UCC_TL_TEAM_SIZE(team), NULL,
            &team->super.super, UCC_TL_UCP_DEFAULT_SCORE,
            ucc_tl_ucp_alg_id_to_init);

        /* If INVALID_PARAM - User provided incorrect input - try to proceed */
        if ((status < 0) && (status != UCC_ERR_INVALID_PARAM) &&
            (status != UCC_ERR_NOT_SUPPORTED)) {
            goto err;
        }
    }

    for (i = 0; i < plugins->n_components; i++) {
        tlcp = ucc_derived_of(plugins->components[i],
                              ucc_tl_coll_plugin_iface_t);
        status = tlcp->get_scores(tl_team, &tlcp_score);
        if (UCC_OK != status) {
            goto err;
        }
        status = ucc_coll_score_merge_in(&score, tlcp_score);
        if (UCC_OK != status) {
            goto err;
        }
    }
    *score_p = score;
    return UCC_OK;
err:
    ucc_coll_score_free(score);
    return status;
}
