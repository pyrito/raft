#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdarg.h>
#include <math.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"
#include "../src/tracing.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(...) Tracef(__VA_ARGS__)
#else
#define tracef(...)
#endif

#define N_SERVERS 3    /* Number of servers in the example cluster */
#define APPLY_RATE 125 /* Apply a new entry every 125 milliseconds */

#define Log(SERVER_ID, FORMAT) fprintf(stderr, "%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
    fprintf(stderr, "%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)

/********************************************************************
 *
 * Sample application FSM that just increases a counter.
 *
 ********************************************************************/

int chunk_size_kb = 0;
int inter_op_interval_ms = 0;
int gdb_print_record_cnt = 0;
int test_duration_secs = 0;
const char *dir;
unsigned id;
int log_level = 1;
bool only_pure_multicast = false;
double alpha = 0.3;

struct Fsm
{
    unsigned long long count;
    double ewma;
    double ewma_latency;
};

struct timeval stop, start, stop_iter, start_iter, apply_start;

uint64_t gdb_print_record_cnt_latency_sum_ms;

static int FsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct Fsm *f = fsm->data;
//    if ((buf->len != chunk_size_kb * 1024) || (strcmp(buf->base, "hi there") != 0)) {
    if ((buf->len != chunk_size_kb *1024)) {    
        fprintf(stderr, "Failing... %s %d\n",
            buf->base, strcmp(buf->base, "hi there"));
        return RAFT_MALFORMED;
    }
    f->count += 1;
    *result = &f->count;

     /* Print out the latency of the apply */
    gettimeofday(&stop_iter, NULL);
    uint64_t apply_end = (stop_iter.tv_sec * 1000) + (stop_iter.tv_usec / 1000);
    uint64_t apply_start = *((uint64_t*)buf->base);
    // TracefL(ERROR, "apply_start=%llums, apply_end=%llums diff=%llums", apply_start, apply_end, apply_end - apply_start);
    gdb_print_record_cnt_latency_sum_ms += apply_end - apply_start;

    if (gdb_print_record_cnt != 0 && (f->count % gdb_print_record_cnt == 0 )) {
      double avg_latency = gdb_print_record_cnt_latency_sum_ms/(float)gdb_print_record_cnt;

      f->ewma_latency = (alpha*avg_latency) + (1-alpha)*f->ewma_latency;
      gdb_print_record_cnt_latency_sum_ms = 0;
      Tracef("Latency for %d records %f EWMA: %f\n", gdb_print_record_cnt, avg_latency, f->ewma_latency);

      unsigned long curr_time =  (stop_iter.tv_sec - start_iter.tv_sec) * 1000 + (stop_iter.tv_usec - start_iter.tv_usec)/1000;
      fprintf(stderr, "Took %ld ms for %d records\n", curr_time, gdb_print_record_cnt);
      gettimeofday(&start_iter, NULL);

      double throughput = (1.0*gdb_print_record_cnt) / (curr_time / 1000.0);
      if (isinf(throughput))
        return 0;

      if (f->ewma == 0.0) {
        f->ewma = throughput;
      } else {
        f->ewma = (alpha*throughput) + (1-alpha)*f->ewma;
      }
      Tracef("Throughput (Ops/sec) %f EWMA: %f\n", throughput, f->ewma);

    }
    return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct Fsm *f = fsm->data;
    *n_bufs = 1;
    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = sizeof(uint64_t);
    (*bufs)[0].base = raft_malloc((*bufs)[0].len);
    if ((*bufs)[0].base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)(*bufs)[0].base = f->count;
    return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(uint64_t)) {
        return RAFT_MALFORMED;
    }
    f->count = *(uint64_t *)buf->base;
    raft_free(buf->base);
    return 0;
}

static int FsmInit(struct raft_fsm *fsm)
{
    struct Fsm *f = raft_malloc(sizeof *f);
    if (f == NULL) {
        return RAFT_NOMEM;
    }
    f->count = 0;
    f->ewma = 0.0;
    fsm->version = 1;
    fsm->data = f;
    fsm->apply = FsmApply;
    fsm->snapshot = FsmSnapshot;
    fsm->restore = FsmRestore;
    return 0;
}

static void FsmClose(struct raft_fsm *f)
{
    if (f->data != NULL) {
        raft_free(f->data);
    }
}

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
    struct uv_timer_s timer;            /* To periodically apply a new entry. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned id;                        /* Raft instance ID. */
    char address[64];                   /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */
};

static void serverRaftCloseCb(struct raft *raft)
{
    struct Server *s = raft->data;
    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);
    FsmClose(&s->fsm);
    if (s->close_cb != NULL) {
        s->close_cb(s);
    }
}

static void serverTransferCb(struct raft_transfer *req)
{
    struct Server *s = req->data;
    raft_id id;
    const char *address;
    raft_leader(&s->raft, &id, &address);
    raft_close(&s->raft, serverRaftCloseCb);
}

/* Final callback in the shutdown sequence, invoked after the timer handle has
 * been closed. */
static void serverTimerCloseCb(struct uv_handle_s *handle)
{
    struct Server *s = handle->data;
    if (s->raft.data != NULL) {
        if (s->raft.state == RAFT_LEADER) {
            int rv;
            //rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
            //if (rv == 0) {
            //    return;
            //}
        }
        raft_close(&s->raft, serverRaftCloseCb);
    }
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
                      struct uv_loop_s *loop,
                      const char *dir,
                      unsigned id)
{
    struct raft_configuration configuration;
    struct timespec now;
    unsigned i;
    int rv;

    memset(s, 0, sizeof *s);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    s->loop = loop;

    /* Add a timer to periodically try to propose a new entry. */
    rv = uv_timer_init(s->loop, &s->timer);
    if (rv != 0) {
        Logf(s->id, "uv_timer_init(): %s", uv_strerror(rv));
        goto err;
    }
    s->timer.data = s;

    /* Initialize the TCP-based RPC transport. */
    rv = raft_uv_tcp_init(&s->transport, s->loop);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the libuv-based I/O backend. */
    rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
    if (rv != 0) {
        Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
        goto err_after_uv_tcp_init;
    }

    /* Initialize the finite state machine. */
    rv = FsmInit(&s->fsm);
    if (rv != 0) {
        Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
        goto err_after_uv_init;
    }

    /* Save the server ID. */
    s->id = id;

    /* Render the address. */
    sprintf(s->address, "127.0.0.1:900%d", id);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
    if (rv != 0) {
        Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
        goto err_after_fsm_init;
    }
    s->raft.data = s;

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    for (i = 0; i < N_SERVERS; i++) {
        char address[64];
        unsigned server_id = i + 1;
        sprintf(address, "127.0.0.1:900%d", server_id);
        rv = raft_configuration_add(&configuration, server_id, address,
                                    RAFT_VOTER);
        if (rv != 0) {
            Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
            goto err_after_configuration_init;
        }
    }
    rv = raft_bootstrap(&s->raft, &configuration);
    if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    raft_set_snapshot_threshold(&s->raft, 64);
    raft_set_snapshot_trailing(&s->raft, 16);
    raft_set_pre_vote(&s->raft, true);

    s->transfer.data = s;

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);
err_after_fsm_init:
    FsmClose(&s->fsm);
err_after_uv_init:
    raft_uv_close(&s->io);
err_after_uv_tcp_init:
    raft_uv_tcp_close(&s->transport);
err:
    return rv;
}

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
    struct Server *s = req->data;
    int count;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
                 status);
        }
        return;
    }
    count = *(int *)result;
}

/* Called periodically every APPLY_RATE milliseconds. */
static void serverTimerCb(uv_timer_t *timer)
{
    struct Server *s = timer->data;
    struct raft_buffer buf;
    struct raft_apply *req;
    int rv;

    struct timeval temp;
    gettimeofday(&temp, NULL);
    if (test_duration_secs < (temp.tv_sec - start.tv_sec)) {
      struct Fsm *f = s->fsm.data;
      fprintf(stderr, "Ops=%d Ops/sec = %f\n",
        f->count, ((float) (f->count))/(temp.tv_sec - start.tv_sec));
      sleep(2);
       exit(0);    
    }
    if (s->raft.state != RAFT_LEADER) {
        return;
    }
    
    gettimeofday(&apply_start, NULL);
    buf.len = chunk_size_kb * 1024;
    buf.base = raft_malloc(buf.len);
    uint64_t curr_time = (apply_start.tv_sec * 1000.0) + (apply_start.tv_usec / 1000.0);
    // fprintf(stderr, "curr_time: %d\n", curr_time);
    *((uint64_t*)buf.base) = curr_time; 
    if (buf.base == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }
    req->data = s;

    rv = raft_apply(&s->raft, req, &buf, 1, serverApplyCb);
    if (rv != 0) {
        Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
        return;
    }
}

/* Start the example server. */
static int ServerStart(struct Server *s)
{
    int rv;

    Log(s->id, "starting");

    rv = raft_start(&s->raft);
    if (rv != 0) {
        Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
        goto err;
    }
    gettimeofday(&start, NULL);
    start_iter = start;
    rv = uv_timer_start(&s->timer, serverTimerCb, 0, inter_op_interval_ms);
    if (rv != 0) {
        Logf(s->id, "uv_timer_start(): %s", uv_strerror(rv));
        goto err;
    }

    return 0;

err:
    return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
    s->close_cb = cb;

    gettimeofday(&stop, NULL);
    Log(s->id, "stopping");

    struct Fsm *f = s->fsm.data;
    fprintf(stderr, "Ops=%d Ops/sec = %f",
      f->count, ((float) (f->count))/(stop.tv_sec - start.tv_sec));
    /* Close the timer asynchronously if it was successfully
     * initialized. Otherwise invoke the callback immediately. */
    if (s->timer.data != NULL) {
        uv_close((struct uv_handle_s *)&s->timer, serverTimerCloseCb);
    } else {
        s->close_cb(s);
    }
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/

static void mainServerCloseCb(struct Server *server)
{
    struct uv_signal_s *sigint = server->data;
    uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
    struct Server *server = handle->data;
    assert(signum == SIGINT);
    uv_signal_stop(handle);
    server->data = handle;
    ServerClose(server, mainServerCloseCb);
}

void parse_options(int argc, char **argv) {
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:t:r:d:a:x:y:l:p")) != -1) {
    switch (opt) {
      case 'c':
        chunk_size_kb = atoi(optarg);
        break;
      case 't':
        inter_op_interval_ms = atoi(optarg);
        break;
      case 'r':
        gdb_print_record_cnt = atoi(optarg);
        break;
      case 'd':
        test_duration_secs = atoi(optarg);
        break;
      case 'a':
        alpha = atof(optarg);
        break;
      case 'x':
        dir = optarg;
        break;
      case 'y':
        id = atoi(optarg);
        break;
      case 'l':
        log_level = atoi(optarg);
        break;
      case 'p':
        only_pure_multicast = true;
        break;
      default: /* '?' */
        fprintf(stderr, "Usage: %s [-c <chunk_size_kb> -t <inter_op_interval_ms> -r <gdb_print_record_cnt> -d <test_duration_secs> -x <dir> -y <id> -l <log_level 1,2,3> -p <only_pure_multicast>]\n",
             argv[0]);
        exit(EXIT_FAILURE);
      }
    }
}

int main(int argc, char *argv[])
{
    parse_options(argc, argv);

    fprintf(stderr, "Test parameters: chunk_size_kb=%d inter_op_interval_ms=%d gdb_print_record_cnt=%d test_duration_secs=%d id=%d log_level=%d only_pure_multicast=%d\n", chunk_size_kb, inter_op_interval_ms, gdb_print_record_cnt, test_duration_secs, id, log_level, only_pure_multicast);

    gdb_print_record_cnt_latency_sum_ms = 0;
    struct uv_loop_s loop;
    struct uv_signal_s sigint; /* To catch SIGINT and exit. */
    struct Server server;
    int rv;

    /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
    signal(SIGPIPE, SIG_IGN);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
        goto err;
    }

    /* Initialize the example server. */
    rv = ServerInit(&server, &loop, dir, id);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Add a signal handler to stop the example server upon SIGINT. */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
        goto err_after_server_init;
    }
    sigint.data = &server;
    rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
    if (rv != 0) {
        Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
        goto err_after_signal_init;
    }

    /* Start the server. */
    rv = ServerStart(&server);
    if (rv != 0) {
        goto err_after_signal_init;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        Logf(id, "uv_run_start(): %s", uv_strerror(rv));
    }

    uv_loop_close(&loop);

    return rv;

err_after_signal_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
    ServerClose(&server, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
err:
    return rv;
}
