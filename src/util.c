#include "../include/raft.h"

const char* message_type_str(int msg_type_enum) {
  switch (msg_type_enum) {
    case RAFT_IO_APPEND_ENTRIES:
      return "RAFT_IO_APPEND_ENTRIES";
    case RAFT_IO_APPEND_ENTRIES_RESULT:
      return "RAFT_IO_APPEND_ENTRIES_RESULT";
    case RAFT_IO_REQUEST_VOTE:
      return "RAFT_IO_REQUEST_VOTE";
    case RAFT_IO_REQUEST_VOTE_RESULT:
      return "RAFT_IO_REQUEST_VOTE_RESULT";
    case RAFT_IO_INSTALL_SNAPSHOT:
      return "RAFT_IO_INSTALL_SNAPSHOT";
    case RAFT_IO_TIMEOUT_NOW:
      return "RAFT_IO_TIMEOUT_NOW";
    case RAFT_IO_HEARTBEAT:
      return "RAFT_IO_HEARTBEAT";
    case RAFT_IO_HEARTBEAT_RESULT:
      return "RAFT_IO_HEARTBEAT_RESULT";
 };
}
