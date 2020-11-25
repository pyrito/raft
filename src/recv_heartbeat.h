/* Receive a Heartbeat message. */

#ifndef RECV_HEARTBEAT_H__
#define RECV_HEARTBEAT_H__

#include "../include/raft.h"

int recvHeartbeat(struct raft *r,
                  raft_id id,
                  const char *address,
                  const struct raft_heartbeat *args);

#endif /* RECV_HEARTBEAT_H__ */
