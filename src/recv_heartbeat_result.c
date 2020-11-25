#include "recv_heartbeat_result.h"

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

static void recvHeartbeatResultCb(struct raft_io_send *req, int status)
{
    (void)status;
    HeapFree(req);
}

int recvHeartbeatResult(struct raft *r,
                        raft_id id,
                        const char *address,
                        const struct raft_heartbeat *args)
{
  struct raft_server *server = &r->configuration.servers[id - 1];
  struct raft_io_send *req;
  int rv;

  // Confirm if the term matches. If not, ignore message.
  if (r->current_term != args->term) {
    TracefL(ERROR, "Got heartbeat result from another term %d, current term is %d", args->term, r->current_term);
    return 0;
  }

  int i = configurationIndexOf(&r->configuration, id);
  progressMarkRecentRecv(r, i);
  return 0;
}

#undef tracef
