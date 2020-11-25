#include "recv_heartbeat.h"

#include "assert.h"
#include "convert.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void recvHeartbeatCb(struct raft_io_send *req, int status)
{
    (void)status;
    HeapFree(req);
}

int recvHeartbeat(struct raft *r,
                  raft_id id,
                  const char *address,
                  const struct raft_heartbeat *args)
{
  struct raft_server *server = &r->configuration.servers[id - 1];
  struct raft_message message;
  struct raft_io_send *req;
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

  req->data = r;
  rv = r->io->send(r->io, req, &message, recvHeartbeatCb);
  if (rv != 0) {
    raft_free(req);
    return rv;
  }

  return 0;
}

#undef tracef
