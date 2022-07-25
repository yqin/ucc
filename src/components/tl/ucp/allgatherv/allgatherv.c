/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "allgatherv.h"
#include "utils/ucc_coll_utils.h"

#ifdef HAVE_DPU_OFFLOAD
#include "dpu_offload_service_daemon.h"
#include "allgatherv_offload.h"

static bool active_colls_initialized = false;

ucc_base_coll_alg_info_t
    ucc_tl_ucp_allgatherv_algs[UCC_TL_UCP_ALLGATHERV_ALG_LAST + 1] = {
        [UCC_TL_UCP_ALLGATHERV_ALG_RING] =
            {.id   = UCC_TL_UCP_ALLGATHERV_ALG_RING,
             .name = "ring",
             .desc = "ring algorithm running on the host"},
        [UCC_TL_UCP_ALLGATHERV_ALG_OFFLOAD] =
            {.id   = UCC_TL_UCP_ALLGATHERV_ALG_OFFLOAD,
             .name = "offload",
             .desc = "offloaded ring algorithm running on the dpu"},
        [UCC_TL_UCP_ALLGATHERV_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

/* calculate the true start address and length of the buffer */
void
get_buffer_range(ucc_coll_args_t *args, ucc_coll_buffer_info_v_t *info_v,
                 ucc_rank_t size, void **start, size_t *len)
{
    size_t dt_size = ucc_dt_size(info_v->datatype);
    int start_offset_i, end_offset_i, start_offset_g, end_offset_g;
    ucc_rank_t i;

    /* initialize global start and end offset to 0 */
    start_offset_g = 0;
    end_offset_g = 0;
    for (i = 0; i < size; i++) {
        start_offset_i = ucc_coll_args_get_displacement(args,
                            info_v->displacements, i) * dt_size;
        if (start_offset_i < start_offset_g) {
            start_offset_g = start_offset_i;
        }
        end_offset_i = start_offset_i +
                        ucc_coll_args_get_count(args, info_v->counts, i) *
                        dt_size;
        if (end_offset_i > end_offset_g) {
            end_offset_g = end_offset_i;
        }
    }

    *len = end_offset_g - start_offset_g;
    *start = info_v->buffer + start_offset_g;
}

/* register xgvmi memory key on the host side */
ucc_status_t
register_memh(ucc_tl_ucp_task_t *task, void *address, size_t length,
              ucp_mem_h *memh)
{
    ucc_tl_ucp_lib_t *lib        = TASK_LIB(task);
    ucp_context_h ucp_context    = TASK_CTX(task)->ucp_context;
    ucp_mem_map_params_t mparams = {0};
    int rc;

    mparams.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                         UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                         UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    mparams.flags      = UCP_MEM_MAP_SHARED;
    mparams.address    = address;
    mparams.length     = length;

    rc = ucp_mem_map(ucp_context, &mparams, memh);
    if (rc) {
        tl_error(lib, "ucp_mem_map failed: %s", ucs_status_string(rc));
        return UCC_ERR_NO_MEMORY;
    }

    return UCC_OK;
}

/* pack rkey buffer */
ucc_status_t pack_rkey(ucc_tl_ucp_task_t *task, ucp_mem_h memh, void **rkey_buf,
                       size_t *buf_size)
{
    ucc_tl_ucp_lib_t *lib = TASK_LIB(task);
    ucp_context_h context = TASK_CTX(task)->ucp_context;
    ucp_mkey_pack_params_t mkey_pack_params = {0};
    int rc;

    mkey_pack_params.field_mask = UCP_MKEY_PACK_PARAM_FIELD_FLAGS;
    mkey_pack_params.flags      = UCP_MKEY_PACK_FLAG_SHARED;
    rc = ucp_mkey_pack(context, memh, &mkey_pack_params, rkey_buf, buf_size);
    if (rc) {
        tl_error(lib, "ucp_mkey_pack failed: %s", ucs_status_string(rc));
        return UCC_ERR_NO_MEMORY;
    }

    return UCC_OK;
}

/* calculate the size of allgatherv_offload_args_t data structure */
size_t
get_offload_args_packed_size(ucc_rank_t comm_size, size_t s_rkey_buf_size,
                             size_t r_rkey_buf_size)
{
    size_t total_size = 0;

    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, coll_type);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, tag);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, group_id);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, size);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, rank);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, padding);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, s_start);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, s_length);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, r_start);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, r_length);
    total_size += comm_size *
        FIELD_SIZEOF(allgatherv_offload_args_t, r_displacements[0]);
    total_size += comm_size *
        FIELD_SIZEOF(allgatherv_offload_args_t, r_counts[0]);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, s_rkey_len);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, r_rkey_len);
    total_size += s_rkey_buf_size;
    total_size += r_rkey_buf_size;

    return total_size;
}

/* pack args to send to DPU */
size_t
pack_allgatherv_offload_args(ucc_tl_ucp_task_t *task, void *s_start,
                             size_t s_length, void *r_start, size_t r_length,
                             void *s_rkey_buf, size_t s_rkey_buf_len,
                             void *r_rkey_buf, size_t r_rkey_buf_len,
                             void *args_buf)
{
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_coll_args_t   *args = &TASK_ARGS(task);
    ucc_rank_t i, size = UCC_TL_TEAM_SIZE(team);
    size_t total_size = 0;
    size_t r_dt_size = ucc_dt_size(args->dst.info_v.datatype);

    *((uint32_t *)(args_buf + total_size)) = args->coll_type;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, coll_type);
    *((uint32_t *)(args_buf + total_size)) = task->tag;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, tag);
    *((uint32_t *)(args_buf + total_size)) = UCC_TL_TEAM_ID(team);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, group_id);
    *((uint32_t *)(args_buf + total_size)) = UCC_TL_TEAM_SIZE(team);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, size);
    *((uint32_t *)(args_buf + total_size)) = UCC_TL_TEAM_RANK(team);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, rank);
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, padding);

    *((uint64_t *)(args_buf + total_size)) = (uint64_t)s_start;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, s_start);
    *((uint64_t *)(args_buf + total_size)) = s_length;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, s_length);

    *((uint64_t *)(args_buf + total_size)) = (uint64_t)r_start;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, r_start);
    *((uint64_t *)(args_buf + total_size)) = r_length;
    total_size += FIELD_SIZEOF(allgatherv_offload_args_t, r_length);

    for (i = 0; i < size; i++) {
        *((uint64_t *)(args_buf + total_size)) =
                ucc_coll_args_get_displacement(args,
                    args->dst.info_v.displacements, i) * r_dt_size;
        total_size += sizeof(uint64_t);
    }

    for (i = 0; i < size; i++) {
        *((uint64_t *)(args_buf + total_size)) =
                ucc_coll_args_get_count(args,
                    args->dst.info_v.counts, i) * r_dt_size;
        total_size += sizeof(uint64_t);
    }

    *((uint64_t *)(args_buf + total_size)) = s_rkey_buf_len;
    total_size += sizeof(uint64_t);
    *((uint64_t *)(args_buf + total_size)) = r_rkey_buf_len;
    total_size += sizeof(uint64_t);

    memcpy(args_buf + total_size, s_rkey_buf, s_rkey_buf_len);
    total_size += s_rkey_buf_len;
    memcpy(args_buf + total_size, r_rkey_buf, r_rkey_buf_len);
    total_size += r_rkey_buf_len;

    return total_size;
}

ucc_status_t ucc_tl_ucp_allgatherv_offload_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t      *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t      *team = TASK_TEAM(task);
    ucc_tl_ucp_lib_t       *lib  = TASK_LIB(task);
    allgatherv_host_coll_t *op   = NULL, *op_item, *op_tmp;

    /* find the associated coll op from active_colls */
    ucc_list_for_each_safe(op_item, op_tmp, &active_colls, super) {
        if (op_item->coll_task == coll_task) {
            /* found a match */
            op = op_item;
            break;
        }
    }
    assert(op);

    if (1 == op->complete) {
        ucc_assert(UCC_TL_UCP_TASK_P2P_COMPLETE(task));
        task->super.super.status = UCC_OK;

        /* deregister xgvmi mkey at end of coll */
        ucp_context_h ucp_context = TASK_CTX(task)->ucp_context;
        int rc;

        rc = ucp_mem_unmap(ucp_context, op->s_memh);
        if (rc) {
            tl_error(lib, "ucp_mem_unmap failed: %s", ucs_status_string(rc));
        }
        rc = ucp_mem_unmap(ucp_context, op->r_memh);
        if (rc) {
            tl_error(lib, "ucp_mem_unmap failed: %s", ucs_status_string(rc));
        }

        /* clean up op since it's no longer needed */
        ucs_list_del(&op->super);
        free(op);
    } else {
        /* progress offload engine */
        execution_context_t *econtext = team->dpu_offloading_econtext;
        econtext->progress(econtext);
    }

    return task->super.super.status;
}

ucc_status_t ucc_tl_ucp_allgatherv_offload_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t      *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t      *team = TASK_TEAM(task);
    ucc_tl_ucp_lib_t       *lib  = TASK_LIB(task);
    ucc_coll_args_t        *args = &TASK_ARGS(task);
    allgatherv_host_coll_t *op   = NULL, *op_item, *op_tmp;
    ucc_status_t            status;
    int                     rc;

    ucc_tl_ucp_task_reset(task);

    /* find the associated coll op from active_colls */
    ucc_list_for_each_safe(op_item, op_tmp, &active_colls, super) {
        if (op_item->coll_task == coll_task) {
            /* found a match */
            op = op_item;
            break;
        }
    }
    assert(op);

    /* get the true start address and buffer length */
    void *s_start = args->src.info.buffer;
    size_t s_len = ucc_dt_size(args->src.info.datatype) * args->src.info.count;
    void *r_start = NULL;
    size_t r_len = 0;
    get_buffer_range(args, &(args->dst.info_v), UCC_TL_TEAM_SIZE(team),
                     &r_start, &r_len);
    assert(args->dst.info_v.buffer == r_start);

    /* register xgvmi mkeys */
    status = register_memh(task, s_start, s_len, &op->s_memh);
    if (status) {
        return UCC_ERR_NO_MEMORY;
    }
    status = register_memh(task, r_start, r_len, &op->r_memh);
    if (status) {
        return UCC_ERR_NO_MEMORY;
    }

    /* pack rkey buffers */
    void *s_rkey_buf, *r_rkey_buf;
    size_t s_rkey_buf_len, r_rkey_buf_len;
    status = pack_rkey(task, op->s_memh, &s_rkey_buf, &s_rkey_buf_len);
    if (status) {
        return UCC_ERR_NO_MEMORY;
    }
    status = pack_rkey(task, op->r_memh, &r_rkey_buf, &r_rkey_buf_len);
    if (status) {
        return UCC_ERR_NO_MEMORY;
    }

    /* calculate buffer size for metadata to send to DPU */
    size_t packed_size = get_offload_args_packed_size(
            UCC_TL_TEAM_SIZE(team), s_rkey_buf_len, r_rkey_buf_len);

    /* get an event from event pool, allocate payload buffer */
    execution_context_t *econtext = team->dpu_offloading_econtext;
    dpu_offload_event_info_t event_info = { .payload_size = packed_size, };
    dpu_offload_event_t *event;
    rc = event_get(econtext->event_channels, &event_info, &event);
    if (rc || !event) {
        tl_error(lib, "event_get() failed");
        return UCC_ERR_NO_MESSAGE;
    }

    /* pack offload args to event payload buffer */
    size_t offload_args_packed_size =
        pack_allgatherv_offload_args(task, s_start, s_len, r_start, r_len,
        s_rkey_buf, s_rkey_buf_len, r_rkey_buf, r_rkey_buf_len, event->payload);
    assert(offload_args_packed_size == packed_size);

    /* rkey_buf is no longer needed */
    ucp_rkey_buffer_release(s_rkey_buf);
    ucp_rkey_buffer_release(r_rkey_buf);

    /* send offload args to DPU */
    rc = event_channel_emit(&event,
                            UCC_TL_UCP_ALLGATHERV_HOST_ARRIVE_AM_ID,
                            GET_SERVER_EP(econtext),
                            econtext->client->server_id,
                            NULL);
    if (rc != EVENT_DONE && rc != EVENT_INPROGRESS) {
        tl_error(lib, "event_channel_emit() failed");
        return UCC_ERR_NO_MESSAGE;
    }

    /* deliver local data */
    int rank = UCC_TL_TEAM_RANK(team);
    size_t r_displacement = ucc_coll_args_get_displacement(args,
                                args->dst.info_v.displacements, rank) *
                            ucc_dt_size(args->dst.info_v.datatype);
    size_t r_size = ucc_coll_args_get_count(args, args->dst.info_v.counts,
                                rank) *
                    ucc_dt_size(args->dst.info_v.datatype);
    assert(s_len <= r_size);
    memcpy(r_start + r_displacement, s_start, r_size);

    /* progress collective once */
    status = ucc_tl_ucp_allgatherv_offload_progress(coll_task);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }

    return ucc_task_complete(coll_task);
}

ucc_status_t ucc_tl_ucp_allgatherv_offload_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *team,
                                                ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_task_t *task = ucc_tl_ucp_init_task(coll_args, team);
    *task_h = &task->super;

    if ((!UCC_DT_IS_PREDEFINED((TASK_ARGS(task)).dst.info_v.datatype)) ||
        (!UCC_IS_INPLACE(TASK_ARGS(task)) &&
         (!UCC_DT_IS_PREDEFINED((TASK_ARGS(task)).src.info.datatype)))) {
        tl_error(UCC_TASK_LIB(task), "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* initialize active_colls if this is the first allgatherv coll */
    if (active_colls_initialized == false) {
        ucs_list_head_init(&active_colls);
        active_colls_initialized = true;
    }

    /* allocate a new coll op for tracking */
    allgatherv_host_coll_t *op = malloc(sizeof(allgatherv_host_coll_t));
    if (!op) {
        ucs_error("not enough memory");
        return UCS_ERR_NO_MEMORY;
    }

    /* initialize coll op and add it to the active_colls */
    op->coll_task = &task->super;
    op->complete  = 0;
    ucs_list_add_tail(&active_colls, &op->super);

    task->super.post     = ucc_tl_ucp_allgatherv_offload_start;
    task->super.progress = ucc_tl_ucp_allgatherv_offload_progress;
    return UCC_OK;
}
#endif // HAVE_DPU_OFFLOAD

ucc_status_t ucc_tl_ucp_allgatherv_ring_init_common(ucc_tl_ucp_task_t *task)
{
    if ((!UCC_DT_IS_PREDEFINED((TASK_ARGS(task)).dst.info_v.datatype)) ||
        (!UCC_IS_INPLACE(TASK_ARGS(task)) &&
         (!UCC_DT_IS_PREDEFINED((TASK_ARGS(task)).src.info.datatype)))) {
        tl_error(UCC_TASK_LIB(task), "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }

    task->super.post     = ucc_tl_ucp_allgatherv_ring_start;
    task->super.progress = ucc_tl_ucp_allgatherv_ring_progress;

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_allgatherv_ring_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t      *team,
                                             ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_task_t *task = ucc_tl_ucp_init_task(coll_args, team);
    *task_h = &task->super;
    return ucc_tl_ucp_allgatherv_ring_init_common(task);
}

ucc_status_t ucc_tl_ucp_allgatherv_init(ucc_tl_ucp_task_t *task)
{
    return ucc_tl_ucp_allgatherv_ring_init_common(task);
}
