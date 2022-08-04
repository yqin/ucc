/**
 * Copyright (C) 2022 NVIDIA Corporation 2022.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef BCAST_OFFLOAD_DPU_H_
#define BCAST_OFFLOAD_DPU_H_

#include <ucs/debug/log_def.h>
#include "dpu_offload_service_daemon.h"
#include "bcast_offload.h"

/* active collectives running on the DPU */
ucs_list_link_t active_colls;

/* arrived RTS notifications that can not be matched due to late receivers */
ucs_list_link_t pending_rts;

/* pending receive subops (RDMA READ) to be posted */
ucs_list_link_t pending_receives;

/* posted receive subops (RDMA READ) */
ucs_list_link_t posted_receives;

/* max concurrent receive subops (RDMA READ) per DPU */
#define max_receives 3

/* unpack coll args received from HOST */
static size_t
unpack_bcast_offload_args(void *data, bcast_offload_args_t *args)
{
    int i;
    size_t total_size = 0;

    args->coll_type = *((uint32_t *)(data + total_size));
    total_size += sizeof(uint32_t);
    args->tag = *((uint32_t *)(data + total_size));
    total_size += sizeof(uint32_t);
    args->group_id = *((uint32_t *)(data + total_size));
    total_size += sizeof(uint32_t);
    args->size = *((uint32_t *)(data + total_size));
    total_size += sizeof(uint32_t);
    args->rank = *((uint32_t *)(data + total_size));
    total_size += sizeof(uint32_t);
    total_size += sizeof(uint32_t) * 3;

    args->s_start = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);
    args->s_length = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);
    args->r_start = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);
    args->r_length = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);

    args->r_displacements = malloc(args->size * sizeof(uint64_t));
    args->r_counts = malloc(args->size * sizeof(uint64_t));
    if (!args->r_displacements || !args->r_counts) {
        ucs_error("not enough memory");
        return 0;
    }

    for (i = 0; i < args->size; i++) {
        args->r_displacements[i] = *((uint64_t *)(data + total_size));
        total_size += sizeof(uint64_t);
    }

    for (i = 0; i < args->size; i++) {
        args->r_counts[i] = *((uint64_t *)(data + total_size));
        total_size += sizeof(uint64_t);
    }

    args->s_rkey_len = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);
    args->r_rkey_len = *((uint64_t *)(data + total_size));
    total_size += sizeof(uint64_t);

    args->s_rkey_buf = malloc(args->s_rkey_len);
    args->r_rkey_buf = malloc(args->r_rkey_len);
    if (!args->s_rkey_buf || !args->r_rkey_buf) {
        ucs_error("not enough memory");
        return 0;
    }

    memcpy(args->s_rkey_buf, data + total_size, args->s_rkey_len);
    total_size += args->s_rkey_len;
    memcpy(args->r_rkey_buf, data + total_size, args->r_rkey_len);
    total_size += args->r_rkey_len;

    return total_size;
}

/* import xgvmi memory key on the dpu side */
static int
import_memh(execution_context_t *context, void *address, size_t length,
            void *rkey_buf, ucp_mem_h *memh)
{
    ucp_context_h ucp_context = context->engine->ucp_context;
    ucp_mem_map_params_t mparams = {0};
    int rc;

    mparams.field_mask          = UCP_MEM_MAP_PARAM_FIELD_FLAGS |
                                  UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                                  UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                                  UCP_MEM_MAP_PARAM_FIELD_SHARED_MKEY_BUFFER;
    mparams.flags               = UCP_MEM_MAP_SHARED;
    mparams.address             = address;
    mparams.length              = length;
    mparams.shared_mkey_buffer  = rkey_buf;

    rc = ucp_mem_map(ucp_context, &mparams, memh);
    if (rc) {
        ucs_error("ucp_mem_map() failed: %s", ucs_status_string(rc));
        return UCS_ERR_NO_MEMORY;
    }

    return UCS_OK;
}

/* TODO: change allgatherv pattern to bcast */
/* create all the send and receive subops */
static int create_bcast_offload_subops(bcast_offload_coll_t *op)
{
    uint32_t size = op->args.size;
    uint32_t rank = op->args.rank;
    uint32_t i, peer;

    /* send - ignore myself which will be completed on the host */
    for (i = 1; i < size; i++) {
        /* actual peer rank */
        peer = (rank + i) % size;
        bcast_offload_subop_t *subop =
            malloc(sizeof(bcast_offload_subop_t));
        if (!subop) {
            ucs_error("not enough memory");
            return UCS_ERR_NO_MEMORY;
        }
        op->s_todo++;
        subop->status = UCS_INPROGRESS;
        subop->type   = BCAST_OFFLOAD_SEND;
        subop->peer   = peer;
        ucs_list_add_tail(&op->s_pending, &subop->super);
    }

    /* receive - ignore myself which will be completed on the host */
    for (i = 1; i < size; i++) {
        /* actual peer rank */
        peer = (rank - i + size) % size;
        bcast_offload_subop_t *subop =
            malloc(sizeof(bcast_offload_subop_t));
        if (!subop) {
            ucs_error("not enough memory");
            return UCS_ERR_NO_MEMORY;
        }
        op->r_todo++;
        subop->status = UCS_INPROGRESS;
        subop->type   = BCAST_OFFLOAD_RECV;
        subop->peer   = peer;
        ucs_list_add_tail(&op->r_pending, &subop->super);
    }

    return UCS_OK;
}

/* calculate the size of bcast_offload_dpu_rts_t data structure */
static size_t get_bcast_offload_dpu_rts_packed_size(size_t rkey_len)
{
    size_t total_size = 0;

    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, coll_type);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, tag);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, group_id);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, rank);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, peer);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, padding);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, start);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, length);
    total_size += FIELD_SIZEOF(bcast_offload_dpu_rts_t, rkey_len);
    total_size += rkey_len;

    return total_size;
}

/* pack DPU RTS notification buffer */
static size_t
pack_bcast_offload_dpu_rts(bcast_offload_coll_t *op,
                                uint32_t peer, void *args_buf)
{
    size_t total_size = 0;

    *((uint32_t *)(args_buf + total_size)) = op->args.coll_type;
    total_size += sizeof(uint32_t);
    *((uint32_t *)(args_buf + total_size)) = op->args.tag;
    total_size += sizeof(uint32_t);
    *((uint32_t *)(args_buf + total_size)) = op->args.group_id;
    total_size += sizeof(uint32_t);
    *((uint32_t *)(args_buf + total_size)) = op->args.rank;
    total_size += sizeof(uint32_t);
    *((uint32_t *)(args_buf + total_size)) = peer;
    total_size += sizeof(uint32_t);
    total_size += sizeof(uint32_t) * 3;

    *((uint64_t *)(args_buf + total_size)) = op->args.s_start;
    total_size += sizeof(uint64_t);
    *((uint64_t *)(args_buf + total_size)) = op->args.s_length;
    total_size += sizeof(uint64_t);
    *((uint64_t *)(args_buf + total_size)) = op->s_rkey_len;
    total_size += sizeof(uint64_t);

    memcpy(args_buf + total_size, op->s_rkey_buf, op->s_rkey_len);
    total_size += op->s_rkey_len;

    return total_size;
}

/* add a matched receive (RDMA READ) subop to pending_receives list */
static int
add_receive(execution_context_t *context, bcast_offload_coll_t *op,
            bcast_offload_subop_t *subop,
            bcast_offload_dpu_rts_t *args)
{
    int rc;

    /* if receive length is 0 mark this receive as completed and send DPU_ACK
     * back to mark send as completed */
#if 0
    /* TODO: */
    if (op->args.r_counts[args->rank] == 0) {
        return UCS_OK;
    }
#endif

    /* create a RDMA READ op and add it to the pending_receives list */
    bcast_offload_rdma_read_t *item =
        malloc(sizeof(bcast_offload_rdma_read_t));
    if (!item) {
        ucs_error("not enough memory");
        return UCS_ERR_NO_MEMORY;
    }

    item->status  = UCS_INPROGRESS;
    item->s_rank  = args->rank;
    item->r_rank  = args->peer;
    item->s_start = args->start;
    item->r_start = op->args.r_start + op->args.r_displacements[args->rank];
    item->length  = op->args.r_counts[args->rank];
    item->r_memh  = op->r_memh;
    item->request = NULL;
    item->op      = op;
    item->subop   = subop;

    /* copy s_rkey buffer */
    item->s_rkey_len = args->rkey_len;
    item->s_rkey_buf = malloc(args->rkey_len);
    if (!item->s_rkey_buf) {
        ucs_error("not enough memory");
        return UCS_ERR_NO_MEMORY;
    }
    memcpy(item->s_rkey_buf, (void *)args +
           FIELD_OFFSET(bcast_offload_dpu_rts_t, rkey_buf),
           args->rkey_len);

    /* now this receive subop is fully initialized and progress engine will pick
     * it up for RDMA READ operation */
    ucs_list_add_tail(&pending_receives, &item->super);

    return UCS_OK;
}

/* complete the whole coll op */
static int
complete_op(execution_context_t *context, bcast_offload_coll_t *op)
{
    int rc;

    op->status = UCS_OK;

    /* get an event from event pool, allocate event buffer */
    size_t packed_size = sizeof(bcast_offload_dpu_done_t);
    execution_context_t *host_context =
        get_server_servicing_host(context->engine);
    assert(host_context);
    dpu_offload_event_info_t event_info = { .payload_size = packed_size, };
    dpu_offload_event_t *event;
    rc = event_get(host_context->event_channels, &event_info, &event);
    if (rc || !event) {
        ucs_error("event_get() failed");
        return UCS_ERR_NO_MESSAGE;
    }

    /* pack DPU DONE notification buffer */
    bcast_offload_dpu_done_t *args_buf = event->payload;
    args_buf->coll_type = op->args.coll_type;
    args_buf->tag       = op->args.tag;
    args_buf->group_id  = op->args.group_id;
    args_buf->rank      = op->args.rank;

    /* send DPU DONE notification to HOST */
    dest_client_t c = GET_CLIENT_BY_RANK(host_context, op->args.group_id,
                                         op->args.rank);
    assert(c.ep);
    rc = event_channel_emit(&event, UCC_TL_UCP_BCAST_DPU_DONE_AM_ID,
                            c.ep, c.id, NULL);
    if (rc != EVENT_DONE && rc != EVENT_INPROGRESS) {
        ucs_error("event_channel_emit() failed");
        return UCS_ERR_NO_MESSAGE;
    }
    ucs_debug("rank %d sent DPU_DONE to host %zu bytes",
              op->args.rank, packed_size);

    /* clean up args */
    free(op->args.r_displacements);
    free(op->args.r_counts);

    /* release rkey buffers */
    ucp_rkey_buffer_release(op->s_rkey_buf);
    ucp_rkey_buffer_release(op->args.s_rkey_buf);
    ucp_rkey_buffer_release(op->args.r_rkey_buf);

    /* deregister xgvmi mkey */
    ucp_context_h ucp_context = context->engine->ucp_context;
    rc = ucp_mem_unmap(ucp_context, op->s_memh);
    if (rc) {
        ucs_error("ucp_mem_unmap() failed: %s", ucs_status_string(rc));
        return UCS_ERR_NO_MESSAGE;
    }
    rc = ucp_mem_unmap(ucp_context, op->r_memh);
    if (rc) {
        ucs_error("ucp_mem_unmap() failed: %s", ucs_status_string(rc));
        return UCS_ERR_NO_MESSAGE;
    }

    assert(ucs_list_is_empty(&op->s_pending));
    assert(ucs_list_is_empty(&op->s_posted));
    assert(ucs_list_is_empty(&op->r_pending));
    assert(ucs_list_is_empty(&op->r_posted));

    /* clean up op since it's no longer needed*/
    ucs_list_del(&op->super);
    free(op);

    return UCS_OK;
}


/* notification callback functions */

/* UCC_TL_UCP_BCAST_HOST_ARRIVE_AM_ID */
static int
host_arrive_am_cb(struct dpu_offload_ev_sys *ev_sys,
                  execution_context_t *context, struct am_header *hdr,
                  size_t hdr_size, void *data, size_t data_len)
{
    ucs_debug("received %zu bytes from client %zu for message type %zu context %p",
              data_len, hdr->id, hdr->type, context);

    int rc;

    /* allocate coll op data structure */
    bcast_offload_coll_t *op = malloc(sizeof(bcast_offload_coll_t));
    if (!op) {
        ucs_error("not enough memory");
        return UCS_ERR_NO_MEMORY;
    }

    /* unpack coll args on DPU */
    bcast_offload_args_t *args = &op->args;
    size_t unpacked_size = unpack_bcast_offload_args(data, args);
    assert(data_len == unpacked_size);

    /* initialize coll op */
    op->status = UCS_INPROGRESS;
    op->s_rkey_len = 0;
    op->s_rkey_buf = NULL;
    op->s_memh = NULL;
    op->r_memh = NULL;
    op->s_todo = 0;
    op->s_done = 0;
    op->r_todo = 0;
    op->r_done = 0;
    ucs_list_head_init(&op->s_pending);
    ucs_list_head_init(&op->s_posted);
    ucs_list_head_init(&op->r_pending);
    ucs_list_head_init(&op->r_posted);

    /* import xgvmi shared key to create alias memory handles */
    rc = import_memh(context, (void *)args->s_start, args->s_length,
                     args->s_rkey_buf, &op->s_memh);
    if (rc) {
        return UCS_ERR_NO_MEMORY;
    }
    rc = import_memh(context, (void *)args->r_start, args->r_length,
                     args->r_rkey_buf, &op->r_memh);
    if (rc) {
        return UCS_ERR_NO_MEMORY;
    }

    /* pack alias rkey buffer to op */
    ucp_context_h ucp_context = context->engine->ucp_context;
    rc = ucp_rkey_pack(ucp_context, op->s_memh, &op->s_rkey_buf,
                       &op->s_rkey_len);
    if (rc) {
        ucs_error("ucp_rkey_pack() failed: %s", ucs_status_string(rc));
    }

    /* prepare subops */
    rc = create_bcast_offload_subops(op);
    if (rc) {
        ucs_error("create_bcast_offload_subops() failed");
        return rc;
    }

    /* add coll op to active list */
    ucs_list_add_tail(&active_colls, &op->super);

    /* post all rts notifications */
    bcast_offload_subop_t *subop = NULL, *subop_item, *subop_tmp;
    ucs_list_for_each_safe(subop_item, subop_tmp, &op->s_pending, super) {
        subop = subop_item;

        /* figure out my remote service process ep */
        dpu_offload_event_t *sp_id_event;
        uint64_t sp_id;
        rc = get_sp_id_by_group_rank(context->engine, args->group_id,
                                     subop->peer, 0, &sp_id, &sp_id_event);
        if (rc || sp_id_event) {
            ucs_error("get_sp_id_by_group_rank() failed");
            return UCS_ERR_NO_MESSAGE;
        }

        ucp_ep_h sp_ep = NULL;
        execution_context_t *sp_context = NULL;
        uint64_t notif_dest_id;
        rc = get_sp_ep_by_id(context->engine, sp_id, &sp_ep, &sp_context,
                             &notif_dest_id);
        if (rc || !sp_ep || !sp_context) {
            ucs_error("get_sp_ep_by_id() failed for SP %" PRIu64 "(rc:%d, sp_ep: %p, sp_context: %p)",
                      sp_id, rc, sp_ep, sp_context);
            return UCS_ERR_NO_MESSAGE;
        }

        /* get an event from event pool, allocate event buffer */
        size_t packed_size = get_bcast_offload_dpu_rts_packed_size(
                                op->s_rkey_len);
        dpu_offload_event_info_t event_info = { .payload_size = packed_size, };
        dpu_offload_event_t *event;
        rc = event_get(sp_context->event_channels, &event_info, &event);
        if (rc || !event) {
            ucs_error("event_get() failed");
            return UCS_ERR_NO_MESSAGE;
        }

        /* pack DPU_RTS notification buffer */
        size_t packed_size_tmp = pack_bcast_offload_dpu_rts(op,
                                    subop->peer, event->payload);
        assert(packed_size == packed_size_tmp);

        /* send RTS notification to remote DPU about data readiness */
        rc = event_channel_emit(&event, UCC_TL_UCP_BCAST_DPU_RTS_AM_ID,
                                sp_ep, notif_dest_id, NULL);
        if (rc != EVENT_DONE && rc != EVENT_INPROGRESS) {
            ucs_error("event_channel_emit() failed");
            return UCS_ERR_NO_MESSAGE;
        }
        ucs_debug("rank %d sent DPU_RTS to sp %zu for peer %d %zu bytes",
                  args->rank, sp_id, subop->peer, packed_size);

        /* move this subop from s_pending to s_posted */
        ucs_list_del(&subop->super);
        ucs_list_add_tail(&op->s_posted, &subop->super);
    }

    return UCS_OK;
}

/* UCC_TL_UCP_BCAST_DPU_RTS_AM_ID */
static int
dpu_rts_am_cb(struct dpu_offload_ev_sys *ev_sys, execution_context_t *context,
              struct am_header *hdr, size_t hdr_size, void *data,
              size_t data_len)
{
    ucs_debug("received %zu bytes from client %zu for message type %zu context %p",
              data_len, hdr->id, hdr->type, context);

    bcast_offload_dpu_rts_t *args = (bcast_offload_dpu_rts_t *)data;
    int rc;

    /* find receiver from active_colls */
    bcast_offload_coll_t *op = NULL, *op_item, *op_tmp;
    ucs_list_for_each_safe(op_item, op_tmp, &active_colls, super) {
        if (op_item->args.coll_type == args->coll_type &&
            op_item->args.tag       == args->tag &&
            op_item->args.group_id  == args->group_id &&
            op_item->args.rank      == args->peer) {
            /* found a match */
            op = op_item;
            break;
        }
    }

    if (op == NULL) {
        /* if not found receiver hasn't arrived yet so put this notification
         * into a global list for later processing from progress engine */
        bcast_offload_dpu_rts_event_t *item =
            malloc(sizeof(bcast_offload_dpu_rts_event_t));
        if (!item) {
            ucs_error("not enough memory");
            return UCS_ERR_NO_MEMORY;
        }

        /* make a copy of payload and add to pending_rts for future process */
        bcast_offload_dpu_rts_t *payload = malloc(data_len);
        memcpy(payload, data, data_len);
        item->payload = payload;
        ucs_list_add_tail(&pending_rts, &item->super);

        return UCS_OK;
    }

    /* find the matched subop from r_pending */
    bcast_offload_subop_t *subop = NULL, *subop_item, *subop_tmp;
    ucs_list_for_each_safe(subop_item, subop_tmp, &op->r_pending, super) {
        if (subop_item->peer == args->rank) {
            /* found a match */
            subop = subop_item;
            break;
        }
    }
    assert(subop);

    /* move this subop from r_pending to r_posted */
    ucs_list_del(&subop->super);
    ucs_list_add_tail(&op->r_posted, &subop->super);

    /* add this receive op to pending_receives list */
    rc = add_receive(context, op, subop, args);
    if (rc) {
        ucs_error("add_receive() failed");
        return UCS_ERR_NO_MESSAGE;
    }

    return UCS_OK;
}

/* UCC_TL_UCP_BCAST_DPU_ACK_AM_ID */
static int
dpu_ack_am_cb(struct dpu_offload_ev_sys *ev_sys, execution_context_t *context,
              struct am_header *hdr, size_t hdr_size, void *data,
              size_t data_len)
{
    ucs_debug("received %zu bytes from client %zu for message type %zu context %p",
              data_len, hdr->id, hdr->type, context);

    bcast_offload_dpu_ack_t *args = (bcast_offload_dpu_ack_t *)data;
    int rc;

    /* find sender from active_colls */
    bcast_offload_coll_t *op = NULL, *op_item, *op_tmp;
    ucs_list_for_each_safe(op_item, op_tmp, &active_colls, super) {
        if (op_item->args.coll_type == args->coll_type &&
            op_item->args.tag       == args->tag &&
            op_item->args.group_id  == args->group_id &&
            op_item->args.rank      == args->peer) {
            /* found a match */
            op = op_item;
            break;
        }
    }
    assert(op);

    /* find subop from s_posted */
    bcast_offload_subop_t *subop = NULL, *subop_item, *subop_tmp;
    ucs_list_for_each_safe(subop_item, subop_tmp, &op->s_posted, super) {
        if (subop_item->peer == args->rank) {
            /* found a match */
            subop = subop_item;
            break;
        }
    }
    assert(subop);

    /* mark this subop as done */
    subop->status = UCS_OK;

    /* increment s_done */
    op->s_done++;

    /* clean up this subop since it's no longer needed */
    ucs_list_del(&subop->super);
    free(subop);

    /* complete the whole coll op if done */
    if (op->s_done == op->s_todo && op->r_done == op->r_todo) {
        rc = complete_op(context, op);
    }

    return UCS_OK;
}

/* register DPU callbacks for event notifications (control path) */
static int register_bcast_dpu_notifications(offloading_engine_t *engine)
{
    int rc;

    rc = engine_register_default_notification_handler(engine,
            UCC_TL_UCP_BCAST_HOST_ARRIVE_AM_ID,
            host_arrive_am_cb);
    if (rc) {
        ucs_error("event_channel_register failed for AM ID %d",
                  UCC_TL_UCP_BCAST_HOST_ARRIVE_AM_ID);
        return rc;
    }

    rc = engine_register_default_notification_handler(engine,
            UCC_TL_UCP_BCAST_DPU_RTS_AM_ID,
            dpu_rts_am_cb);
    if (rc) {
        ucs_error("event_channel_register failed for AM ID %d",
                  UCC_TL_UCP_BCAST_DPU_RTS_AM_ID);
        return rc;
    }

    rc = engine_register_default_notification_handler(engine,
            UCC_TL_UCP_BCAST_DPU_ACK_AM_ID,
            dpu_ack_am_cb);
    if (rc) {
        ucs_error("event_channel_register failed for AM ID %d",
                  UCC_TL_UCP_BCAST_DPU_ACK_AM_ID);
        return rc;
    }

    return UCS_OK;
}

/* progress pending RTS */
static int progress_rts(execution_context_t *context)
{
    int rc;

    /* check pending RTS, if found creat a new receive op */
    bcast_offload_dpu_rts_event_t *rts = NULL, *rts_item, *rts_tmp;
    bcast_offload_coll_t *op = NULL, *op_item, *op_tmp;
    bcast_offload_subop_t *subop = NULL, *subop_item, *subop_tmp;
    ucs_list_for_each_safe(rts_item, rts_tmp, &pending_rts, super) {
        ucs_list_for_each_safe(op_item, op_tmp, &active_colls, super) {
            if (rts_item->payload->coll_type == op_item->args.coll_type &&
                rts_item->payload->tag       == op_item->args.tag &&
                rts_item->payload->group_id  == op_item->args.group_id &&
                rts_item->payload->peer      == op_item->args.rank) {
                /* found a matched op */
                op = op_item;

                ucs_list_for_each_safe(subop_item, subop_tmp, &op->r_pending,
                                       super) {
                    if (rts_item->payload->rank == subop_item->peer) {
                        /* found a matched subop */
                        subop = subop_item;
                        rts = rts_item;

                        /* add this receive op to pending_receives list */
                        rc = add_receive(context, op, subop, rts->payload);
                        if (rc) {
                            ucs_error("add_receive() failed");
                            return UCS_ERR_NO_MESSAGE;
                        }

                        /* rts_item is no longer needed so can be released */
                        free(rts->payload);
                        ucs_list_del(&rts->super);
                        free(rts);

                        /* if subop is matched no need to continue */
                        break;
                    }
                }
                assert(subop);

                /* if op is matched move on to the next rts_item */
                break;
            }
        }
    }

    return UCS_OK;
}

/* progress receives */
static int progress_receive(execution_context_t *context)
{
    int rc;

    /* find completed receive ops */
    bcast_offload_rdma_read_t *recv = NULL, *recv_item, *recv_tmp;
    bcast_offload_coll_t *op;
    bcast_offload_subop_t *subop;
    ucs_list_for_each_safe(recv_item, recv_tmp, &posted_receives, super) {
        /* check completion of the request */
        ucs_status_t status;
        if (recv_item->request == NULL) {
            status = UCS_OK;
        } else {
            status = ucp_request_check_status(recv_item->request);
        }
        if (status == UCS_OK) {
            /* completed */
            recv = recv_item;

            /* 0. mark this receive as done */
            recv->status = status;
            free(recv->s_rkey_buf);
            if (recv->request) {
                ucp_request_free(recv->request);
            }
            recv->request = NULL;

            op = recv->op;
            subop = recv->subop;

            /* mark this subop as done */
            subop->status = UCS_OK;

            /* increment r_done */
            op->r_done++;

            /* 1. send DPU_ACK notification to remote service process */
            /* figure out my remote SP ep */
            dpu_offload_event_t *sp_id_event;
            uint64_t sp_id;
            rc = get_sp_id_by_group_rank(context->engine, op->args.group_id,
                                         subop->peer, 0, &sp_id, &sp_id_event);
            if (rc || sp_id_event) {
                ucs_error("get_dpu_id_by_group_rank() failed");
                return UCS_ERR_NO_MESSAGE;
            }

            ucp_ep_h sp_ep = NULL;
            execution_context_t *sp_context = NULL;
            uint64_t notif_dest_id;
            rc = get_sp_ep_by_id(context->engine, sp_id, &sp_ep, &sp_context,
                                 &notif_dest_id);
            if (rc || !sp_ep || !sp_context)
            {
                ucs_error("get_sp_ep_by_id() failed for SP %" PRIu64 "(rc:%d, sp_ep: %p, sp_context: %p)",
                          sp_id, rc, sp_ep, sp_context, sp_context->type);
                return UCS_ERR_NO_MESSAGE;
            }

            /* get an event from event pool, allocate event buffer */
            size_t packed_size = sizeof(bcast_offload_dpu_ack_t);
            dpu_offload_event_info_t event_info = {
                .payload_size = packed_size,
            };

            dpu_offload_event_t *sp_event;
            rc = event_get(sp_context->event_channels, &event_info, &sp_event);
            if (rc || !sp_event) {
                ucs_error("event_get() failed");
                return UCS_ERR_NO_MESSAGE;
            }

            /* pack DPU_ACK notification buffer */
            bcast_offload_dpu_ack_t *args_buf =
                (bcast_offload_dpu_ack_t *)sp_event->payload;
            args_buf->coll_type = op->args.coll_type;
            args_buf->tag       = op->args.tag;
            args_buf->group_id  = op->args.group_id;
            args_buf->rank      = op->args.rank;
            args_buf->peer      = subop->peer;

            /* send DPU_ACK notification to remote DPU */
            rc = event_channel_emit(&sp_event,
                                    UCC_TL_UCP_BCAST_DPU_ACK_AM_ID,
                                    sp_ep, notif_dest_id, NULL);
            if (rc != EVENT_DONE && rc != EVENT_INPROGRESS) {
                ucs_error("event_channel_emit() failed");
                return UCS_ERR_NO_MESSAGE;
            }
            ucs_debug("rank %d sent DPU_ACK to sp %zu for peer %d %zu bytes",
                      op->args.rank, sp_id, subop->peer, packed_size);

            /* 2. clean up this subop since it's no longer needed */
            ucs_list_del(&subop->super);
            free(subop);

            /* remove this receive */
            ucs_list_del(&recv->super);
            free(recv);

            /* 3. complete the whole coll op if done */
            if (op->s_done == op->s_todo && op->r_done == op->r_todo) {
                rc = complete_op(context, op);
            }
        }
    }

    /* progress RDMA READ (receive) ops - post up to max_receives each time */
    int posted_count = ucs_list_length(&posted_receives);
    for (int i = 0; (i < max_receives - posted_count) &&
                    !ucs_list_is_empty(&pending_receives); i++) {
        /* extract one item from pending_receives */
        bcast_offload_rdma_read_t *item =
            ucs_list_extract_head(&pending_receives,
                                  bcast_offload_rdma_read_t, super);

        /* move this item to posted_receives */
        ucs_list_add_tail(&posted_receives, &item->super);

        /* figure out my remote service process ep */
        dpu_offload_event_t *sp_id_event;
        uint64_t sp_id;
        rc = get_sp_id_by_group_rank(context->engine, item->op->args.group_id,
                                     item->s_rank, 0, &sp_id, &sp_id_event);
        if (rc || sp_id_event) {
            ucs_error("get_sp_id_by_group_rank() failed");
            return UCS_ERR_NO_MESSAGE;
        }

        ucp_ep_h sp_ep = NULL;
        execution_context_t *sp_context = NULL;
        uint64_t notif_dest_id;
        rc = get_sp_ep_by_id(context->engine, sp_id, &sp_ep, &sp_context,
                             &notif_dest_id);
        if (rc || !sp_ep || !sp_context) {
            ucs_error("get_sp_ep_by_id() failed for SP %" PRIu64, sp_id);
            return UCS_ERR_NO_MESSAGE;
        }

        /* unpack rkey */
        ucp_rkey_h s_rkey;
        rc = ucp_ep_rkey_unpack(sp_ep, item->s_rkey_buf, &s_rkey);
        if (rc) {
            ucs_error("ucp_ep_rkey_unpack() failed: %s", ucs_status_string(rc));
            return UCS_ERR_NO_MESSAGE;
        }

        /* prepare lkey */
        ucp_request_param_t param = {
            .op_attr_mask = UCP_OP_ATTR_FIELD_MEMH,
            .memh = item->r_memh,
        };

        /* post RDMA READ */
        ucs_status_ptr_t req = NULL;
        req = ucp_get_nbx(sp_ep, (void *)item->r_start, item->length,
                          item->s_start, s_rkey, &param);
        if (UCS_PTR_IS_ERR(req)) {
            ucs_error("ucp_get_nbx() failed with error %d", UCS_PTR_STATUS(req));
            return UCS_ERR_NO_MESSAGE;
        }
        item->request = req;
    }

    return UCS_OK;
}

/* progress all offload operations */
static int progress_ops(execution_context_t *context)
{
    int rc;
    rc = progress_rts(context);
    rc = progress_receive(context);

    return UCS_OK;
}
#endif //BCAST_OFFLOAD_DPU_H_
