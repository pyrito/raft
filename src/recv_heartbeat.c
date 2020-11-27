#include "recv_heartbeat.h"

#include "assert.h"
#include "convert.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(__VA_ARGS__)
#else
#define tracef(...)
#endif

/* Context of a RAFT_IO_HEARTBEAT request that was submitted with
 * raft_io_>send(). */
struct sendHeartbeatResult
{
    struct raft *raft;          /* Instance sending the entries. */
    struct raft_io_send send;   /* Underlying I/O send request. */
    raft_id server_id;          /* Destination server. */
};

static void sendHeartbeatResultCb(struct raft_io_send *send, int status)
{
    struct sendHeartbeatResult *req = send->data;
    if (status != 0)
      TracefL(ERROR, "Failed to send heartbeat result to %d %s", req->server_id,
        errCodeToString(status));

    HeapFree(req);
}

int recvHeartbeat(struct raft *r,
                  raft_id id,
                  const char *address,
                  const struct raft_heartbeat *args)
{
  struct raft_server *server = &r->configuration.servers[id - 1];
  struct raft_message message;
  struct sendHeartbeatResult *req;
  int rv;

  // Confirm if the term matches. If not, ignore message.
  if (r->current_term != args->term) {
    TracefL(ERROR, "Got heartbeat from another term %d, current term is %d", args->term, r->current_term);
    return 0;
  }

  /* Reset the election timer. */
  r->election_timer_start = r->io->time(r->io);
  message.type = RAFT_IO_HEARTBEAT_RESULT;
  message.server_id = id;
  message.server_address = address;
  message.heartbeat_result.term = r->current_term;

  req = raft_malloc(sizeof *req);
  if (req == NULL) {
    TracefL(ERROR, "Out of memory");
    rv = RAFT_NOMEM;
    return rv;
  }

  req->raft = r;
  req->server_id = server->id;
  req->send.data = req;

  rv = r->io->send(r->io, &req->send, &message, sendHeartbeatResultCb);
  if (rv != 0) {
    raft_free(req);
    return rv;
  }

  return 0;
}

#undef tracef
