/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include "../include/raft.h"
#include "time.h"

/* Default no-op tracer. */
extern struct raft_tracer NoopTracer;

char time_buffer[26];
unsigned long long millisecondsSinceEpoch;
struct timeval tv;

/* Emit a debug message with the given tracer. */
#ifdef NOTRACE
#define Tracef(...)                                     \
    do {                                                \
    } while(0)
#else
#define Tracef(...)                                     \
    do {                                                \
        char _msg[1024];                                \
        snprintf(_msg, sizeof _msg, __VA_ARGS__);       \
        gettimeofday(&tv, NULL);                        \
        millisecondsSinceEpoch =                        \
          (unsigned long long)(tv.tv_sec) * 1000 +      \
            (unsigned long long)(tv.tv_usec) / 1000;    \
        fprintf(stderr, "%ld %s:%d: %s\n", millisecondsSinceEpoch, __FILE__, __LINE__, _msg);\
    } while (0)

#define TracefL(LEVEL, ...)                                     \
    do {                                                \
        char _msg[1024];                                \
        snprintf(_msg, sizeof _msg, __VA_ARGS__);       \
        gettimeofday(&tv, NULL);                        \
        millisecondsSinceEpoch =                        \
          (unsigned long long)(tv.tv_sec) * 1000 +      \
            (unsigned long long)(tv.tv_usec) / 1000;    \
        if (log_level >= LEVEL)                         \
          fprintf(stderr, "%ld %s:%d: %s\n", millisecondsSinceEpoch, __FILE__, __LINE__, _msg);\
    } while (0)

#endif

#endif /* TRACING_H_ */
