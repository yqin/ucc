/**
 * Copyright (C) 2022 NVIDIA Corporation 2022.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef ALLGATHERV_OFFLOAD_H_
#define ALLGATHERV_OFFLOAD_H_

/*
 * This header file contains all information shared between the client (HOST)
 * and the server (DPU) so only basic data types should be used (including UCX
 * data types), but not derived UCC data types.
 */

enum allgatherv_ext_am_id {
    /*
     * AM message from HOST to DPU to notify rank has arrived and DPU can start.
     * This message should contain all meta data for both send and receive. Send
     * and receive buffers should be pre-registered with xgvmi and the mkeys are
     * part of the message.
     */
    UCC_TL_UCP_ALLGATHERV_HOST_ARRIVE_AM_ID = 601,

    /*
     * AM message from local DPU to remote DPU to notify remote DPU that I'm
     * ready to send. This message should contain remote HOST rank, xgvmi key,
     * address and length (all info for RDMA read).
     */
    UCC_TL_UCP_ALLGATHERV_DPU_RTS_AM_ID,

    /*
     * AM message from remote DPU to local DPU to notify local DPU that the RDMA
     * read has completed. This message should contain the HOST rank of the
     * receive.
     */
    UCC_TL_UCP_ALLGATHERV_DPU_ACK_AM_ID,

    /*
     * AM message from DPU to HOST to notify a particular HOST rank that all its
     * sends and receives have been completed to mark the collective completion
     * for the rank.
     */
    UCC_TL_UCP_ALLGATHERV_DPU_DONE_AM_ID,

    /* Last AM message ID */
    UCC_TL_UCP_ALLGATHERV_LAST_AM_ID
};

enum allgatherv_offload_subop_type {
    ALLGATHERV_OFFLOAD_SEND = 0,
    ALLGATHERV_OFFLOAD_RECV = 1
};

#define FIELD_SIZEOF(_t, _f) (sizeof(((_t*)0)->_f))
#define FIELD_OFFSET(_t, _f) ((unsigned long)&(((_t*)0)->_f))

/* Below defines all the data structures for the AM notification */

/*
 * Data structure that HOST sends to DPU when it arrives. This contains all
 * information for the DPU to start the allgatherv operation.
 */
typedef struct allgatherv_offload_args {
    uint32_t  coll_type;        /* collective type */
    uint32_t  tag;              /* collective ID/TAG */
    uint32_t  group_id;         /* collective comm/group/team ID */
    uint32_t  size;             /* collective comm/group/team size */
    uint32_t  rank;             /* rank in the comm/group/team */
    uint32_t  padding[3];       /* padding */
    /* 32 bytes total before this line */
    uint64_t  s_start;          /* send buffer true start address */
    uint64_t  s_length;         /* send buffer memory region length */
    uint64_t  r_start;          /* receive buffer true start address */
    uint64_t  r_length;         /* receive buffer memory region length */
    uint64_t *r_displacements;  /* receive displacements in bytes */
    uint64_t *r_counts;         /* receive count in bytes */
    uint64_t  s_rkey_len;       /* rkey buffer length for send buffer */
    uint64_t  r_rkey_len;       /* rkey buffer length for receive buffer */
    /* 8-byte aligned before this line */
    void     *s_rkey_buf;       /* shared rkey buffer for send buffer */
    void     *r_rkey_buf;       /* shared rkey buffer for receive buffer */
} allgatherv_offload_args_t;

/*
 * Data structure that DPU sends to remote DPU containing all information for an
 * RDMA read operation.
 */
typedef struct allgatherv_offload_dpu_rts {
    uint32_t  coll_type;        /* collective type */
    uint32_t  tag;              /* collective ID/TAG */
    uint32_t  group_id;         /* collective comm/group/team ID */
    uint32_t  rank;             /* rank in the comm/group/team */
    uint32_t  peer;             /* peer that I can send data to */
    uint32_t  padding[3];       /* padding */
    /* 32 bytes total before this line */
    uint64_t  start;            /* send start address (with displacement) */
    uint64_t  length;           /* send data length */
    uint64_t  rkey_len;         /* rkey buffer length for send buffer */
    /* 8-byte aligned before this line */
    void     *rkey_buf;         /* rkey buffer for send buffer */
} allgatherv_offload_dpu_rts_t;

/*
 * Data structure that remote DPU sends to local DPU to acknowledge a receive is
 * complete for a particular rank.
 */
typedef struct allgatherv_offload_dpu_ack {
    uint32_t  coll_type;        /* collective type */
    uint32_t  tag;              /* collective ID/TAG */
    uint32_t  group_id;         /* collective comm/group/team ID */
    uint32_t  rank;             /* rank in the comm/group/team */
    uint32_t  peer;             /* rank that I received data from */
} allgatherv_offload_dpu_ack_t;

/* Data structure that DPU sends to HOST to mark the completion for the rank. */
typedef struct allgatherv_offload_dpu_done {
    uint32_t  coll_type;        /* collective type */
    uint32_t  tag;              /* collective ID/TAG */
    uint32_t  group_id;         /* collective comm/group/team ID */
    uint32_t  rank;             /* rank in the comm/group/team */
} allgatherv_offload_dpu_done_t;

/*
 * Collective data structure that needs to be maintained through the life cycle
 * of the collective operation, make sure to release all temporary data 
 * properly when collective is completed.
 */
typedef struct allgatherv_offload_coll {
    ucs_list_link_t               super;        /* op list item */
    ucs_status_t                  status;       /* op status */
    allgatherv_offload_args_t      args;         /* coll args for this op */
    size_t                        s_rkey_len;   /* length of s_rkey_buf */
    void                         *s_rkey_buf;   /* s_rkey packed buffer */
    ucp_mem_h                     s_memh;       /* memh for send buffer */
    ucp_mem_h                     r_memh;       /* memh for receive buffer */
    int                           s_todo;       /* number of sends to do */
    int                           s_done;       /* completed sends */
    int                           r_todo;       /* number of receives to do*/
    int                           r_done;       /* completed receives */
    ucs_list_link_t               s_pending;    /* pending send list */
    ucs_list_link_t               s_posted;     /* posted send list */
    ucs_list_link_t               r_pending;    /* pending receive list */
    ucs_list_link_t               r_posted;     /* posted receive list */
} allgatherv_offload_coll_t;

/* Suboperation list item for bookkeeping of the send or receive operation. */
typedef struct allgatherv_offload_subop {
    ucs_list_link_t     super;      /* subop list item */
    ucs_status_t        status;     /* subop status */
    uint32_t            type;       /* subop type - send or receive */
    uint32_t            peer;       /* host peer to send to or receive from */
} allgatherv_offload_subop_t;

/* DPU RTS notification bookkeeping list item. */
typedef struct allgatherv_offload_dpu_rts_event {
    ucs_list_link_t                  super;      /* notification list item */
    allgatherv_offload_dpu_rts_t     *payload;    /* payload buffer */
} allgatherv_offload_dpu_rts_event_t;

/* RDMA READ bookkeeping list item. */
typedef struct allgatherv_offload_rdma_read {
    ucs_list_link_t              super;      /* op list item */
    ucs_status_t                 status;     /* op status */
    uint32_t                     s_rank;     /* send rank */
    uint32_t                     r_rank;     /* receiver rank */
    uint64_t                     s_start;    /* send start address */
    uint64_t                     r_start;    /* receive start address */
    uint64_t                     length;     /* data length */
    size_t                       s_rkey_len; /* length of s_rkey_buf */
    void                        *s_rkey_buf; /* s_rkey packed buffer */
    ucp_mem_h                    r_memh;     /* memh for receive buffer */
    void                        *request;    /* request associated with subop */
    allgatherv_offload_coll_t    *op;         /* original receive op */
    allgatherv_offload_subop_t   *subop;      /* original receive subop */
} allgatherv_offload_rdma_read_t;
#endif //ALLGATHERV_OFFLOAD_H_
