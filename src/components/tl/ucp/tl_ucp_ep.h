/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"

#ifndef UCC_TL_UCP_EP_H_
#define UCC_TL_UCP_EP_H_
#include "ucc/api/ucc.h"
#include <ucp/api/ucp.h>
#include "tl_ucp.h"
#include "core/ucc_team.h"

#if HAVE_DPU_OFFLOAD
#include "dpu_offload_event_channels.h"
#endif

typedef struct ucc_tl_ucp_context ucc_tl_ucp_context_t;
typedef struct ucc_tl_ucp_team    ucc_tl_ucp_team_t;

/*
#define RANK_SHADOW_DPU_EP(_ctx, _rank, _dpu_idx)                              \
    ({                                                                         \
        ucp_ep_h __ep;                                                         \
        __ep = _ctx->shadow_eps[_rank][_dpu_idx];                              \
        __ep;                                                                  \
    })
*/

#if HAVE_DPU_OFFLOAD
ucc_status_t ucc_tl_ucp_connect_team_shadow_ep(ucc_tl_ucp_team_t *team,
                                               ucc_rank_t         team_rank,
                                               int       shadow_dpu_idx,
                                               ucp_ep_h *ep);
#endif // HAVE_DPU_OFFLOAD

ucc_status_t ucc_tl_ucp_connect_team_ep(ucc_tl_ucp_team_t *team,
                                        ucc_rank_t team_rank, ucp_ep_h *ep);

void ucc_tl_ucp_close_eps(ucc_tl_ucp_context_t *ctx);

static inline ucc_context_addr_header_t *
ucc_tl_ucp_get_team_ep_header(ucc_tl_ucp_team_t *team, ucc_rank_t core_rank)

{
    return ucc_get_team_ep_header(UCC_TL_CORE_CTX(team), UCC_TL_CORE_TEAM(team),
                                  core_rank);
}

#ifdef HAVE_DPU_OFFLOAD
#define DPU_OFFLOADING_EXECUTION_CONTEXT(team)                                 \
    ({                                                                         \
        execution_context_t *_econtext = NULL;                                 \
        if (team != NULL)                                                      \
            _econtext = team->dpu_offloading_econtext;                         \
        _econtext;                                                             \
    })

#define DPU_OFFLOADING_ENGINE(team)                                            \
    ({                                                                         \
        offloading_engine_t *_e = NULL;                                        \
        if (team != NULL) {                                                    \
            execution_context_t *_econtext = team->dpu_offloading_econtext;    \
            if (_econtext != NULL)                                             \
                _e = _econtext->engine;                                        \
        }                                                                      \
        _e;                                                                    \
    })

#define DPU_OFFLOADING_EXECUTION_CONTEXT_ID(team)                              \
    ({                                                                         \
        uint64_t             _id;                                              \
        execution_context_t *_econtext;                                        \
        if (team != NULL) {                                                    \
            _econtext = team->dpu_offloading_econtext;                         \
            if (_econtext != NULL)                                             \
                _id = ECONTEXT_ID(_econtext);                                  \
        }                                                                      \
        _id;                                                                   \
    })

#if 0
static inline ucc_status_t
ucc_tl_ucp_request_shadow_dpu_data(ucc_tl_ucp_team_t *team, ucc_rank_t rank,
                                   void **req)
{
    dpu_offload_event_t  *ev;
    dpu_offload_status_t  rc;
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_tl_ucp_lib_t     *lib = UCC_TL_UCP_TEAM_LIB(team);
    uint16_t              team_id = team->super.super.params.id;
    dpu_offload_event_info_t ev_info;
    ev_info.payload_size = sizeof(rank_info_t);

    rc = event_get(DPU_OFFLOADING_EXECUTION_CONTEXT(team)->event_channels,
                   &ev_info, &ev);
    if (rc)
    {
        tl_error(lib, "event_get() failed");
        return UCC_ERR_NO_MESSAGE;
    }
    assert(ev != NULL);
    rank_info_t *shadow_ep_req_data = (rank_info_t*)ev->payload;
    shadow_ep_req_data->group_id   = team_id;
    shadow_ep_req_data->group_rank = rank;

    rc = event_channel_emit(&ev, AM_PEER_CACHE_REQ_MSG_ID,
            RANK_SHADOW_DPU_EP(ctx, UCC_TL_TEAM_RANK(team), 0), NULL);
    if (rc) {
        /* FIXME: do we need to handle EVENT_INPROGRESS? */
        tl_error(lib, "event_channel_emit() failed with %d", rc);
        return UCC_ERR_NO_MESSAGE;
    }
    *req = ev->req;
    return UCC_OK;
}

#define NUM_SHADOWS_DPUS_UNKNOWN (-1)
static inline ucc_status_t
ucc_tl_ucp_get_shadow_dpu_eps(ucc_tl_ucp_team_t *team, ucc_rank_t rank,
                              int dpu_index, ucp_ep_h **eps, size_t *num_eps)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);

    if (ctx->num_shadow_eps[rank] != NUM_SHADOWS_DPUS_UNKNOWN) {
        *eps     = ctx->shadow_eps[rank];
        *num_eps = ctx->num_shadow_eps[rank];
        return UCC_OK;
    }

    *eps     = NULL;
    *num_eps = NUM_SHADOWS_DPUS_UNKNOWN;
    return UCC_OK;
}
#endif
#endif // HAVE_DPU_OFFLOAD

static inline ucc_status_t ucc_tl_ucp_get_ep(ucc_tl_ucp_team_t *team,
                                             ucc_rank_t rank, ucp_ep_h *ep)
{
    ucc_tl_ucp_context_t      *ctx      = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_addr_header_t *h        = NULL;
    ucc_rank_t                 ctx_rank = 0;
    ucc_status_t               status;
    ucc_rank_t                 core_rank;

    core_rank = ucc_ep_map_eval(UCC_TL_TEAM_MAP(team), rank);
    if (ctx->eps) {
        ucc_team_t *core_team = UCC_TL_CORE_TEAM(team);
        /* Core super.super.team ptr is NULL for service_team
           which has scope == UCC_CL_LAST + 1*/
        ucc_assert((NULL != core_team) || IS_SERVICE_TEAM(team));
        ctx_rank =
            core_team ? ucc_get_ctx_rank(core_team, core_rank) : core_rank;
        *ep = ctx->eps[ctx_rank];
    } else {
        h   = ucc_tl_ucp_get_team_ep_header(team, core_rank);
        *ep = tl_ucp_hash_get(ctx->ep_hash, h->ctx_id);
    }
    if (NULL == (*ep)) {
        /* Not connected yet */
        status = ucc_tl_ucp_connect_team_ep(team, core_rank, ep);
        if (ucc_unlikely(UCC_OK != status)) {
            tl_error(UCC_TL_TEAM_LIB(team), "failed to connect team ep");
            *ep = NULL;
            return status;
        }
        if (ctx->eps) {
            ctx->eps[ctx_rank] = *ep;
        } else {
            tl_ucp_hash_put(ctx->ep_hash, h->ctx_id, *ep);
        }
    }
    return UCC_OK;
}

#endif
