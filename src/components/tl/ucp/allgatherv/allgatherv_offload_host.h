/**
 * Copyright (C) 2022 NVIDIA Corporation 2022.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef ALLGATHERV_OFFLOAD_HOST_H_
#define ALLGATHERV_OFFLOAD_HOST_H_

#include "dpu_offload_service_daemon.h"
#include "allgatherv.h"
#include "allgatherv_offload.h"

/* notification callback functions */

/* UCC_TL_UCP_ALLGATHERV_DPU_DONE_AM_ID */
static int
dpu_done_am_cb(struct dpu_offload_ev_sys *ev_sys, execution_context_t *context,
               struct am_header *hdr, size_t hdr_size, void *data,
               size_t data_len)
{
    ucs_debug("received %zu bytes from client %zu for message type %zu context %p",
              data_len, hdr->id, hdr->type, context);

    allgatherv_offload_dpu_done_t *args = (allgatherv_offload_dpu_done_t *)data;

    /* find op from active_colls */
    allgatherv_host_coll_t *op = NULL, *op_item, *op_tmp;
    ucs_list_for_each_safe(op_item, op_tmp, &active_colls, super)
    {
        ucc_tl_ucp_task_t *task      = ucc_derived_of(op_item->coll_task,
                                                      ucc_tl_ucp_task_t);
        ucc_tl_ucp_team_t *team      = TASK_TEAM(task);
        ucc_coll_args_t   *coll_args = &TASK_ARGS(task);
        uint32_t           rank      = UCC_TL_TEAM_RANK(team);
        group_uid_t        group_uid = team->offloading_uid;
        assert(group_uid != INT_MAX);
        if (coll_args->coll_type == args->coll_type &&
            task->tag            == args->tag &&
            group_uid            == args->group_uid &&
            rank                 == args->rank) {
            /* found a match */
            op = op_item;
            break;
        }
    }
    assert(op);

    /* mark offload completion */
    op->complete = 1;

    return UCC_OK;
}

/* register HOST callbacks for event notifications (control path) */
static int register_allgatherv_host_notifications(dpu_offload_ev_sys_t *ev_sys)
{
    int rc;

    rc = event_channel_register(ev_sys,
                                UCC_TL_UCP_ALLGATHERV_DPU_DONE_AM_ID,
                                dpu_done_am_cb, NULL);
    if (rc) {
        ucs_error("event_channel_register() failed for AM ID %d",
                  UCC_TL_UCP_ALLGATHERV_DPU_DONE_AM_ID);
        return rc;
    }

    return UCC_OK;
}
#endif //ALLGATHERV_OFFLOAD_HOST_H_
