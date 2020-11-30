/* Receive a Heartbeat message. */

#ifndef RECV_RELINK_H__
#define RECV_RELINK_H__

#include "../include/raft.h"

int recvRelink(struct raft *r,
               raft_id id,
               const char *address,
               const struct raft_relink *args);

#endif /* RECV_RELINK_H__ */
