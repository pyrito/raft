#include "progress.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(__VA_ARGS__)
#else
#define tracef(...)
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Initialize a single progress object. */
static void initProgress(struct raft_progress *p, raft_index last_index, raft_time node_alive_start, raft_id id, int num_nodes)
{
    p->next_index = last_index + 1;
    p->match_index = 0;
    p->snapshot_index = 0;
    p->last_append_entries_send = 0;
    p->recent_recv = false;

    // TODO - Leader failures are not handled properly with respect to chain replication.
    // Below is a hack for testing chain healing without leader failure.
    p->state = PROGRESS__PROBE;
    p->next_sibling_id = 0;
    p->node_alive_start = node_alive_start;
    p->recent_alive_recv = false;
    p->one_probe_outstanding = false;
}

int progressBuildArray(struct raft *r)
{
    struct raft_progress *progress;
    unsigned i;
    raft_index last_index = logLastIndex(&r->log);
    progress = raft_malloc(r->configuration.n * sizeof *progress);
    raft_time now = r->io->time(r->io);
    if (progress == NULL) {
        return RAFT_NOMEM;
    }
    for (i = 0; i < r->configuration.n; i++) {
        initProgress(&progress[i], last_index, now, r->configuration.servers[i].id, r->configuration.n);
        if (r->configuration.servers[i].id == r->id) {
            progress[i].match_index = r->last_stored;
        }
    }
    r->leader_state.progress = progress;
    return 0;
}

int progressRebuildArray(struct raft *r,
                         const struct raft_configuration *configuration)
{
    raft_index last_index = logLastIndex(&r->log);
    struct raft_progress *progress;
    unsigned i;
    unsigned j;
    raft_id id;

    progress = raft_malloc(configuration->n * sizeof *progress);
    if (progress == NULL) {
        return RAFT_NOMEM;
    }

    /* First copy the progress information for the servers that exists both in
     * the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        id = r->configuration.servers[i].id;
        j = configurationIndexOf(configuration, id);
        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }
        progress[j] = r->leader_state.progress[i];
    }

    raft_time now = r->io->time(r->io);
    /* Then reset the replication state for servers that are present in the new
     * configuration, but not in the current one. */
    for (i = 0; i < configuration->n; i++) {
        id = configuration->servers[i].id;
        j = configurationIndexOf(&r->configuration, id);
        if (j < r->configuration.n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }
        assert(j == r->configuration.n);
        initProgress(&progress[i], last_index, now, r->configuration.servers[i].id, r->configuration.n);
    }

    TracefL(ERROR, "Big error!!!!! Who did the reconfig?");
    raft_free(r->leader_state.progress);
    r->leader_state.progress = progress;

    return 0;
}

bool progressIsUpToDate(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    raft_index last_index = logLastIndex(&r->log);
    return p->next_index == last_index + 1;
}

const char* progStateToStr(int state) {
    switch (state) {
        case PROGRESS__SNAPSHOT:
            return "PROGRESS__SNAPSHOT";
        case PROGRESS__PROBE:
            return "PROGRESS__PROBE";
        case PROGRESS__PIPELINE:
            return "PROGRESS__PIPELINE";
        default:
           return "INVALID_STATE";
    }
}

bool progressShouldReplicate(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    raft_time now = r->io->time(r->io);
    bool has_append_entries_interval_passed = now - p->last_append_entries_send >= r->heartbeat_timeout;
    //TracefL(DEBUG, "has_append_entries_interval_passed = %d = %llu - %llu >= %llu", has_append_entries_interval_passed,
    //  now, p->last_append_entries_send, r->heartbeat_timeout);
    raft_index last_index = logLastIndex(&r->log);
    bool result = false;

    if (r->next_sibling_id != 0 && p->next_sibling_id != 0) {
      // Node in chain replication list.
      if (r->configuration.servers[i].id != r->next_sibling_id) {
        TracefL(DEBUG, "Should replicate for arr_idx %d state=%s? False, since node is in chain but not the next sibling",
          i, progStateToStr(p->state));
        return false;
      }
    }

    /* We must be in a valid state. */
    assert(p->state == PROGRESS__PROBE || p->state == PROGRESS__PIPELINE ||
           p->state == PROGRESS__SNAPSHOT);

    /* The next index to send must be lower than the highest index in our
     * log. */
    assert(p->next_index <= last_index + 1);

    switch (p->state) {
        case PROGRESS__SNAPSHOT:
            /* If we have already sent a snapshot, don't send any further entry
             * and let's wait for the target server to reply.
             *
             * TODO: rollback to probe if we don't hear anything for a while. */
            result = false;
            break;
        case PROGRESS__PROBE:
            /* We send at most one message per heatbeat interval. */
            // Option 2 -
            result = !progressIsUpToDate(r, i) && !r->leader_state.progress[i].one_probe_outstanding;
            // Option 1 -
            //result = !progressIsUpToDate(r, i) && has_append_entries_interval_passed;
            break;
        case PROGRESS__PIPELINE:
            /* In replication mode we send empty append entries messages only if
             * haven't sent anything in the last heartbeat interval. */
            result = !progressIsUpToDate(r, i);
            break;
    }
    TracefL(DEBUG, "Should replicate for arr_idx %d state=%s? last_local_idx=%d next_index=%d, has_append_entries_interval_passed=%d progressIsUpToDate=%d verdict=%d",
      i, progStateToStr(p->state), last_index, p->next_index, has_append_entries_interval_passed, progressIsUpToDate(r, i), result);
    return result;
}

raft_index progressNextIndex(struct raft *r, unsigned i)
{
    return r->leader_state.progress[i].next_index;
}

raft_index progressMatchIndex(struct raft *r, unsigned i)
{
    return r->leader_state.progress[i].match_index;
}

void progressUpdateLastSend(struct raft *r, unsigned i)
{
    r->leader_state.progress[i].last_append_entries_send = r->io->time(r->io);
    TracefL(DEBUG, "In progressUpdateLastSend for node index %d last_append_entries_send %llu", i, r->leader_state.progress[i].last_append_entries_send);
}

bool progressResetRecentRecv(struct raft *r, const unsigned i)
{
    bool prev = r->leader_state.progress[i].recent_recv;
    r->leader_state.progress[i].recent_recv = false;
    return prev;
}

void progressMarkRecentRecv(struct raft *r, const unsigned i)
{
    r->leader_state.progress[i].recent_recv = true;
}

bool progressResetRecentAliveRecv(struct raft *r, const unsigned i)
{
    bool prev = r->leader_state.progress[i].recent_alive_recv;
    r->leader_state.progress[i].recent_alive_recv = false;
    return prev;
}

void progressMarkRecentAliveRecv(struct raft *r, const unsigned i)
{
    r->leader_state.progress[i].recent_alive_recv = true;
}

void progressToSnapshot(struct raft *r, unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->state = PROGRESS__SNAPSHOT;
    p->snapshot_index = logSnapshotIndex(&r->log);
}

void progressAbortSnapshot(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->snapshot_index = 0;
    p->state = PROGRESS__PROBE;
}

int progressState(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    return p->state;
}

bool progressMaybeDecrement(struct raft *r,
                            const unsigned i,
                            raft_index rejected,
                            raft_index last_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];

    assert(p->state == PROGRESS__PROBE || p->state == PROGRESS__PIPELINE ||
           p->state == PROGRESS__SNAPSHOT);

    if (p->state == PROGRESS__SNAPSHOT) {
        /* The rejection must be stale or spurious if the rejected index does
         * not match the last snapshot index. */
        if (rejected != p->snapshot_index) {
            return false;
        }
        progressAbortSnapshot(r, i);
        return true;
    }

    if (p->state == PROGRESS__PIPELINE) {
        /* The rejection must be stale if the rejected index is smaller than
         * the matched one. */
        if (rejected <= p->match_index) {
            tracef("match index is up to date -> ignore ");
            return false;
        }
        /* Directly decrease next to match + 1 */
        p->next_index = min(rejected, p->match_index + 1);
        progressToProbe(r, i);
        return true;
    }

    /* The rejection must be stale or spurious if the rejected index does not
     * match the next index minus one. */
    if (rejected != p->next_index - 1) {
        tracef("rejected index %llu different from next index %lld -1 -> ignore ",
               rejected, p->next_index);
        return false;
    }

    p->next_index = min(rejected, last_index + 1);
    p->next_index = max(p->next_index, 1);

    return true;
}

void progressOptimisticNextIndex(struct raft *r,
                                 unsigned i,
                                 raft_index next_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->next_index = next_index;
}

bool progressMaybeUpdate(struct raft *r, unsigned i, raft_index last_index)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    bool updated = false;
    if (p->match_index < last_index) {
        p->match_index = last_index;
        updated = true;
    }
    if (p->next_index < last_index + 1) {
        p->next_index = last_index + 1;
    }
    return updated;
}

void progressToProbe(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];

    /* If the current state is snapshot, we know that the pending snapshot has
     * been sent to this peer successfully, so we probe from snapshot_index +
     * 1.*/
    if (p->state == PROGRESS__SNAPSHOT) {
        assert(p->snapshot_index > 0);
        p->next_index = max(p->match_index + 1, p->snapshot_index);
        p->snapshot_index = 0;
    } else {
        p->next_index = p->match_index + 1;
    }
    p->state = PROGRESS__PROBE;
    // Piyush TODO - What if a node disconnects and then re-connects.
    // This flag should be reset to false in that case.
    r->leader_state.progress[i].one_probe_outstanding = false;
}

void progressToPipeline(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    p->state = PROGRESS__PIPELINE;
}

bool progressSnapshotDone(struct raft *r, const unsigned i)
{
    struct raft_progress *p = &r->leader_state.progress[i];
    assert(p->state == PROGRESS__SNAPSHOT);
    return p->match_index >= p->snapshot_index;
}

#undef tracef
