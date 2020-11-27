#include "recv_heartbeat_result.h"

#include "assert.h"
#include "convert.h"
#include "heap.h"
#include "log.h"
#include "progress.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(__VA_ARGS__)
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

  int idx = configurationIndexOf(&r->configuration, id);
  if (r->leader_state.progress[idx].state == PROGRESS__DEAD) {
    // TODO: Add node back in. Replenish it to at least the index entry of the leader's next sibling and
    // add it to the end of the chain.
    r->leader_state.progress[idx].state = PROGRESS__CHAIN_HOLE_REPLINISH;
    
  }

  int i = configurationIndexOf(&r->configuration, id);
  progressMarkRecentRecv(r, i);
  progressMarkRecentAliveRecv(r, i);
  return 0;
}

#undef tracef
