#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "membership.h"
#include "progress.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(__VA_ARGS__)
#else
#define tracef(...)
#endif

/* Apply time-dependent rules for followers (Figure 3.1). */
static int tickFollower(struct raft *r)
{
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER);

    server = configurationGet(&r->configuration, r->id);

    /* If we have been removed from the configuration, or maybe we didn't
     * receive one yet, just stay follower. */
    if (server == NULL) {
        return 0;
    }

    /* Check if we need to start an election.
     *
     * From Section 3.3:
     *
     *   If a follower receives no communication over a period of time called
     *   the election timeout, then it assumes there is no viable leader and
     *   begins an election to choose a new leader.
     *
     * Figure 3.1:
     *
     *   If election timeout elapses without receiving AppendEntries RPC from
     *   current leader or granting vote to candidate, convert to candidate.
     */
    if (electionTimerExpired(r) && server->role == RAFT_VOTER) {
        TracefL(INFO, "convert to candidate and start new election");
        rv = convertToCandidate(r, false /* disrupt leader */);
        if (rv != 0) {
            tracef("convert to candidate: %s", raft_strerror(rv));
            return rv;
        }
    }

    return 0;
}

/* Apply time-dependent rules for candidates (Figure 3.1). */
static int tickCandidate(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_CANDIDATE);

    /* Check if we need to start an election.
     *
     * From Section 3.4:
     *
     *   The third possible outcome is that a candidate neither wins nor loses
     *   the election: if many followers become candidates at the same time,
     *   votes could be split so that no candidate obtains a majority. When this
     *   happens, each candidate will time out and start a new election by
     *   incrementing its term and initiating another round of RequestVote RPCs
     */
    if (electionTimerExpired(r)) {
        tracef("start new election");
        return electionStart(r);
    }

    return 0;
}

/* Return true if we received an AppendEntries RPC result from a majority of
 * voting servers since we became leaders or since the last time this function
 * was called.
 *
 * For each server the function checks the recent_recv flag of the associated
 * progress object, and resets the flag after the check. It returns true if a
 * majority of voting server had the flag set to true. */
static bool checkContactQuorum(struct raft *r)
{
    unsigned i;
    unsigned contacts = 0;
    assert(r->state == RAFT_LEADER);

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        bool recent_recv = progressResetRecentRecv(r, i);
        if ((server->role == RAFT_VOTER && recent_recv) ||
            server->id == r->id) {
            contacts++;
        }
    }

    return contacts > configurationVoterCount(&r->configuration) / 2;
}

raft_id findNodeWithNextSibling(struct raft *r, raft_id id) {
  for (int i = 0; i < r->configuration.n; i++) {
    if (r->leader_state.progress[i].next_sibling_id == id)
      return r->configuration.servers[i].id;
  }
  // TODO - Shouldn't reach here. Enable asserts and test if asserts are working
  // by moving this line to the start of this function.
  assert(false);
}

/* Context of a RAFT_IO_RELINK request that was submitted with
 * raft_io_>send(). */
struct sendRelink
{
    struct raft *raft;          /* Instance sending the entries. */
    struct raft_io_send send;   /* Underlying I/O send request. */
    int rescuer_idx;          /* Rescuer idx. */
    raft_id rescuer_id;
    raft_id victim_id;          /* Victim id. */
};

static void sendRelink(struct raft *r, int rescuer_idx, raft_id rescuer_id, raft_id victim_id);
static void sendRelinkCb(struct raft_io_send *send, const int status);

/* Callback invoked after request to send relink RPC has completed. */
static void sendRelinkCb(struct raft_io_send *send, const int status)
{
    struct sendRelink *req = send->data;
    struct raft *r = req->raft;
    if (status != 0) {
      TracefL(ERROR, "Failed to send relink to idx %d %s", req->rescuer_idx,
        errCodeToString(status));
      // TODO - Add a sleep here!
      sendRelink(r, req->rescuer_idx, req->rescuer_id, req->victim_id);
    }
    else {
      // Update the leader's view of the chain. Set next_sibling for node at rescuer_id to the victim's id.
      r->leader_state.progress[req->rescuer_idx].next_sibling_id = req->victim_id;
    }

    raft_free(req);
}

static void sendRelink(struct raft *r, int rescuer_idx, raft_id rescuer_id, raft_id victim_id) {
    struct raft_message message;
    struct raft_relink *args = &message.relink;
    struct sendRelink *req;
    int rv;

    if (PROGRESS__DEAD == r->leader_state.progress[rescuer_idx].state) {
      return;
    }

    TracefL(INFO, "Sending relink to rescuer %d with victim id %d", rescuer_id, victim_id);
    args->term = r->current_term;
    args->next_sibling_id = victim_id;

    message.type = RAFT_IO_RELINK;
    message.server_id = rescuer_id;
    message.server_address = r->configuration.servers[rescuer_idx].address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_NOMEM;
        TracefL(ERROR, "Failed to send relink message to rescuer id %d for victim id %d because of no memory", rescuer_id, victim_id);
        // TODO - Add a sleep here!
        sendRelink(r, rescuer_idx, rescuer_id, victim_id);
    }
    req->raft = r;
    req->rescuer_idx = rescuer_idx;
    req->victim_id = victim_id;

    req->send.data = req;
    rv = r->io->send(r->io, &req->send, &message, sendRelinkCb);
    if (rv != 0) {
        TracefL(ERROR, "Failed to send relink message to rescuer id %d for victim id %d", rescuer_id, victim_id);
        raft_free(req);
        sendRelink(r, rescuer_idx, rescuer_id, victim_id);
    }
}

void handleDeadNode(struct raft *r, int i) {
    TracefL(INFO, "Node %d is down. Relink!!!", r->configuration.servers[i].id);
    r->leader_state.progress[i].state = PROGRESS__DEAD;

    // Perform relinking steps.
    raft_id rescuer_id = findNodeWithNextSibling(r, r->configuration.servers[i].id);
    int rescuer_idx = configurationIndexOf(&r->configuration, rescuer_id);
    raft_id victim_id = r->leader_state.progress[i].next_sibling_id;
    int victim_idx = configurationIndexOf(&r->configuration, victim_id);
    int next_sibling_idx = configurationIndexOf(&r->configuration, r->next_sibling_id);

    // Mark victim node's progress to signify that it's hole has to be filled with
    // respect to the rescuer node. Also, note that index of entry till which we should fill the victim
    // to ensure no hole will exist when the rescuer starts relaying messages to the victim.
    // Note that this will also ensure that the leader doesn't replicate more entries to non victim nodes
    // till holes are filled at victims.
    if (victim_id != r->id) {
      TracefL(INFO, "The victim is the leader, don't replenish");
      r->leader_state.progress[victim_idx].state = PROGRESS__CHAIN_HOLE_REPLINISH;
      r->leader_state.progress[victim_idx].replenish_till_index = r->leader_state.progress[next_sibling_idx].next_index - 1;
    }

    if (rescuer_id == r->id) {
      r->next_sibling_id = victim_id;

      // Reset the next sibling id for the failed node.
      r->leader_state.progress[i].next_sibling_id = -1;
    } else {
      // Update the leader's view of the chain. Set next_sibling for node at rescuer_id to the victim's id.
      // NOTE: Update this before the relink occurs at the rescuer. Why? Think about the case where the rescuer dies before
      // the relink message reaches it.
      r->leader_state.progress[rescuer_idx].next_sibling_id = victim_id;

      // Reset the next sibling id for the failed node.
      r->leader_state.progress[i].next_sibling_id = -1;

      // Notify the rescuer id node to conform to leader's view of the chain.
      // NOTE: This has to be the last call in this method. Don't do anything after this without caution.
      sendRelink(r, rescuer_idx, rescuer_id, victim_id);
    }
}

/* Apply time-dependent rules for leaders (Figure 3.1). */
static int tickLeader(struct raft *r)
{
    raft_time now = r->io->time(r->io);
    assert(r->state == RAFT_LEADER);

    /* Check if we still can reach a majority of servers.
     *
     * From Section 6.2:
     *
     *   A leader in Raft steps down if an election timeout elapses without a
     *   successful round of heartbeats to a majority of its cluster; this
     *   allows clients to retry their requests with another server.
     */
    if (now - r->election_timer_start >= r->election_timeout) {
        if (!checkContactQuorum(r)) {
            tracef("unable to contact majority of cluster -> step down");
            convertToFollower(r);
            return 0;
        }
        r->election_timer_start = r->io->time(r->io);
    }

    for (int i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        struct raft_progress *progress = &(r->leader_state.progress[i]);
        if (server->id == r->id || progress->state == PROGRESS__DEAD) {
            continue;
        }
        TracefL(INFO, "Checking alive timer for node idx %d with id %d, node_alive_start=%d node_alive_timeout=%d now=%d recent_alive_recv=%d",
          i, server->id, progress->node_alive_start, r->node_alive_timeout, now, progress->recent_alive_recv);
        if ((now - progress->node_alive_start >= r->node_alive_timeout)) {
          if (!progress->recent_alive_recv) {
            handleDeadNode(r, i);
          } else {
            progress->node_alive_start = r->io->time(r->io);
          }
          progressResetRecentAliveRecv(r, i);
        }
    }

    if (now - r->election_timer_start >= r->election_timeout) {
        if (!checkContactQuorum(r)) {
            tracef("unable to contact majority of cluster -> step down");
            convertToFollower(r);
            return 0;
        }
        r->election_timer_start = r->io->time(r->io);
    }

    /* Possibly send heartbeats.
     *
     * From Figure 3.1:
     *
     *   Send empty AppendEntries RPC during idle periods to prevent election
     *   timeouts.
     */
    replicationHeartbeat(r);
    replicationAppendEntries(r);

    /* If a server is being promoted, increment the timer of the current
     * round or abort the promotion.
     *
     * From Section 4.2.1:
     *
     *   The algorithm waits a fixed number of rounds (such as 10). If the last
     *   round lasts less than an election timeout, then the leader adds the new
     *   server to the cluster, under the assumption that there are not enough
     *   unreplicated entries to create a significant availability
     *   gap. Otherwise, the leader aborts the configuration change with an
     *   error.
     */
    if (r->leader_state.promotee_id != 0) {
        raft_id id = r->leader_state.promotee_id;
        unsigned server_index;
        raft_time round_duration = now - r->leader_state.round_start;
        bool is_too_slow;
        bool is_unresponsive;

        /* If a promotion is in progress, we expect that our configuration
         * contains an entry for the server being promoted, and that the server
         * is not yet considered as voting. */
        server_index = configurationIndexOf(&r->configuration, id);
        assert(server_index < r->configuration.n);
        assert(r->configuration.servers[server_index].role != RAFT_VOTER);

        is_too_slow = (r->leader_state.round_number == r->max_catch_up_rounds &&
                       round_duration > r->election_timeout);
        is_unresponsive = round_duration > r->max_catch_up_round_duration;

        /* Abort the promotion if we are at the 10'th round and it's still
         * taking too long, or if the server is unresponsive. */
        if (is_too_slow || is_unresponsive) {
            struct raft_change *change;

            r->leader_state.promotee_id = 0;

            r->leader_state.round_index = 0;
            r->leader_state.round_number = 0;
            r->leader_state.round_start = 0;

            change = r->leader_state.change;
            r->leader_state.change = NULL;
            if (change != NULL && change->cb != NULL) {
                change->cb(change, RAFT_NOCONNECTION);
            }
        }
    }

    return 0;
}

static int tick(struct raft *r)
{
    int rv = -1;

    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

    /* If we are not available, let's do nothing. */
    if (r->state == RAFT_UNAVAILABLE) {
        return 0;
    }

    switch (r->state) {
        case RAFT_FOLLOWER:
            rv = tickFollower(r);
            break;
        case RAFT_CANDIDATE:
            rv = tickCandidate(r);
            break;
        case RAFT_LEADER:
            rv = tickLeader(r);
            break;
    }

    return rv;
}

void tickCb(struct raft_io *io)
{
    struct raft *r;
    int rv;
    r = io->data;
    rv = tick(r);
    if (rv != 0) {
        convertToUnavailable(r);
        return;
    }

    /* For all states: if there is a leadership transfer request in progress,
     * check if it's expired. */
    if (r->transfer != NULL) {
        raft_time now = r->io->time(r->io);
        if (now - r->transfer->start >= r->election_timeout) {
            membershipLeadershipTransferClose(r);
        }
    }
}

#undef tracef
