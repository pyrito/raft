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
    raft_id id;
    raft_id next_sibling_id;          /* Victim id. */
    int chain_incarnation_id;
};

static int sendRelink(struct raft *r, raft_id id, raft_id next_sibling_id);
static void sendRelinkCb(struct raft_io_send *send, const int status);

/* Callback invoked after request to send relink RPC has completed. */
static void sendRelinkCb(struct raft_io_send *send, const int status)
{
    struct sendRelink *req = send->data;
    struct raft *r = req->raft;
    TracefL(INFO, "sendRelinkCb for node %d", req->id);

    if (status != 0 && r->chain_incarnation_id == req->chain_incarnation_id) {
      TracefL(ERROR, "Failed to send relink to id %d %s", req->id,
        errCodeToString(status));
      switchToPureMulticast(r);
    }

    raft_free(req);
}

static int sendRelink(struct raft *r, raft_id id, raft_id next_sibling_id) {
    struct raft_message message;
    struct raft_relink *args = &message.relink;
    struct sendRelink *req;
    int rv;

    TracefL(INFO, "Sending relink to node id %d with next sibling id %d", id, next_sibling_id);
    args->term = r->current_term;
    args->next_sibling_id = next_sibling_id;

    message.type = RAFT_IO_RELINK;
    message.server_id = id;
    message.server_address = r->configuration.servers[configurationIndexOf(&r->configuration, id)].address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_NOMEM;
        TracefL(ERROR, "Failed to send relink message to id %d for next_sibling_id %d because of no memory", id, next_sibling_id);
        return rv;
    }
    req->raft = r;
    req->next_sibling_id = next_sibling_id;
    req->id = id;
    req->chain_incarnation_id = r->chain_incarnation_id;

    req->send.data = req;
    rv = r->io->send(r->io, &req->send, &message, sendRelinkCb);
    if (rv != 0) {
        TracefL(ERROR, "Failed to send relink message to id %d for next_sibling_id %d", id, next_sibling_id);
        raft_free(req);
        return rv;
    }
    return 0;
}

void switchToPureMulticast(struct raft *r) {
   TracefL(ERROR, "Chain modification: switchToPureMulticast");
   r->next_sibling_id = 0;
   r->should_send_to_next_sibling = 0;
   for (int i = 0; i < r->configuration.n; i++) {
     struct raft_progress *progress = &(r->leader_state.progress[i]);
     progress->next_sibling_id = 0;
   }
   TracefL(ERROR, "Chain modification: Finished switching to pure multicast");
}

void handleDeadNode(struct raft *r, int i) {
    if (r->leader_state.progress[i].next_sibling_id != 0) {
      TracefL(INFO, "Node %d is down.", r->configuration.servers[i].id);
      switchToPureMulticast(r);
    }

    //r->leader_state.progress[i].state = PROGRESS__DEAD;
}

typedef struct chainNode {
  raft_id id;
  raft_index next_index;
} chainNode;

int compChainNode(const void *elem1, const void *elem2) 
{
  
  return ((chainNode*)elem1)->next_index >= ((chainNode*)elem2)->next_index;
}

bool moreHealthyChainExists(struct raft *r) {
   int current_chain_len = 0;
   if (r->should_send_to_next_sibling) {
     current_chain_len = 1;
     int crwl_id = r->next_sibling_id;
     while (crwl_id != r->id) {
       current_chain_len++;
       int idx = configurationIndexOf(&r->configuration, crwl_id);
       crwl_id = r->leader_state.progress[idx].next_sibling_id;
     }
   }

   int cnt_alive_nodes_including_leader = 1;
   for (int i = 0; i < r->configuration.n; i++) {
     struct raft_progress *progress = &(r->leader_state.progress[i]);
     if(r->configuration.servers[i].id != r->id && !progress->dead)
       cnt_alive_nodes_including_leader++; 
   }

   if (cnt_alive_nodes_including_leader > current_chain_len && cnt_alive_nodes_including_leader >= 3) {
     TracefL(ERROR, "Chain modification: moreHealthyChainExists cnt_alive_nodes_including_leader=%d current_chain_len=%d", cnt_alive_nodes_including_leader, current_chain_len);
     return true;
   }
   return false;
}

void reformChain(struct raft *r) {
   /*
    1. Check if a chain of significant length can be formed out of non-DEAD nodes.
    2. Pause sendAppendEntries.
    3. Send re-link messages.
    4. If all re-link messages pass, update progress struct to mark next_sibling for nodes. Else, return.

    When sending sendAppendEntries, add a flag to indicate if the next_sibling is supposed to be used or not.
   */
   // TODO - Don't always relink alive nodes. What if they have index entries far apart?
   int cnt_alive_nodes_excluding_leader = 0;
   for (int i = 0; i < r->configuration.n; i++) {
     struct raft_progress *progress = &(r->leader_state.progress[i]);
     if (!progress->dead && r->configuration.servers[i].id != r->id)
       cnt_alive_nodes_excluding_leader++; 
   }

   TracefL(INFO, "Chain modification: Num alive nodes: %d", cnt_alive_nodes_excluding_leader);
   raft_index *next_indices;
   chainNode *nodes;
   nodes = (chainNode *) malloc(sizeof(chainNode) * cnt_alive_nodes_excluding_leader);

   int crwl = 0;
   for (int i = 0; i < r->configuration.n; i++) {
      struct raft_progress *progress = &(r->leader_state.progress[i]);
      if (!progress->dead && r->configuration.servers[i].id != r->id) {
         nodes[crwl].id = r->configuration.servers[i].id;
         nodes[crwl].next_index = r->leader_state.progress[i].next_index;
         crwl++;
      }
   }
 
   qsort(nodes, cnt_alive_nodes_excluding_leader, sizeof(chainNode), compChainNode);

   r->chain_incarnation_id++;

   char* node_ids_str = malloc(3*cnt_alive_nodes_excluding_leader*sizeof(char));
   if (node_ids_str == NULL) {
     TracefL(ERROR, "Failed to malloc");
   }
   memset(node_ids_str, 0, 3*cnt_alive_nodes_excluding_leader*sizeof(char));
   for (int i = 0; i <cnt_alive_nodes_excluding_leader; i++) {
     TracefL(INFO, "Chain modification: Alive node %d", nodes[i].id);
     char str[12] = {0};
     sprintf(str, "%d, ", nodes[i].id);
     strcat(node_ids_str, str);
   }

   TracefL(ERROR, "Chain modification: Reforming chain with alive nodes %s", node_ids_str);

   // Send relink messages
   for (int i=0; i<cnt_alive_nodes_excluding_leader-1; i++) {
     TracefL(INFO, "Calling relink for nodes[i].id: %d, next_sibling: %d", nodes[i].id,  nodes[(i+1)%cnt_alive_nodes_excluding_leader].id);
     if (sendRelink(r, nodes[i].id, nodes[(i+1)%cnt_alive_nodes_excluding_leader].id) != 0) {
       return;
     } 
   }
   if (sendRelink(r, nodes[cnt_alive_nodes_excluding_leader-1].id, r->id) != 0) {
     return;
   }
  
   // Set the next_sibling in progress struct
   for (int i = 0; i <cnt_alive_nodes_excluding_leader-1; i++) {
      int node_idx = configurationIndexOf(&r->configuration, nodes[i].id);
      struct raft_progress *progress = &(r->leader_state.progress[node_idx]);
      progress->next_sibling_id = nodes[(i+1)%cnt_alive_nodes_excluding_leader].id;
   }

   int node_idx = configurationIndexOf(&r->configuration, nodes[cnt_alive_nodes_excluding_leader-1].id);
   struct raft_progress *progress = &(r->leader_state.progress[node_idx]);
   progress->next_sibling_id = r->id;

   r->next_sibling_id = nodes[0].id;

   r->should_send_to_next_sibling = 1;

   // Reset next_indices to what we saw when we sorted
   for (int i = 0; i <cnt_alive_nodes_excluding_leader; i++) {
      int node_idx = configurationIndexOf(&r->configuration, nodes[i].id);
      struct raft_progress *progress = &(r->leader_state.progress[node_idx]);
      progress->next_index = nodes[i].next_index;
   }

   TracefL(ERROR, "Chain modification: New chain incarnation id %d with nodes: %s\n", r->chain_incarnation_id, node_ids_str);
   // TODO - Change update of next_index in recvAppendEntriesResult path. Check both
   // optimistic and non-optimistic cases.

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
        if (server->id == r->id) {
            continue;
        }
        //TracefL(INFO, "Checking alive timer for node idx %d with id %d, node_alive_start=%d node_alive_timeout=%d now=%d recent_alive_recv=%d, state=%d",
        //  i, server->id, progress->node_alive_start, r->node_alive_timeout, now, progress->recent_alive_recv, progress->state);
        if ((now - progress->node_alive_start >= r->node_alive_timeout)) {
          if (!progress->recent_alive_recv) {
            progress->dead = true;
            handleDeadNode(r, i);
          } else {
            progress->node_alive_start = r->io->time(r->io);
            progress->dead = false;
          }
          progressResetRecentAliveRecv(r, i);
        }
    }

    if (moreHealthyChainExists(r)) {
      TracefL(INFO, "We found a healthier chain, switching to pure multicast");
      switchToPureMulticast(r); 
      reformChain(r);
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
