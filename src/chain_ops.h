/* Chain replication related operations */

#ifndef CHAIN_REPLICATION_H_
#define CHAIN_REPLICATION_H_

#include "../include/raft.h"

void switchToPureMulticast(struct raft *r);

#endif /* CHAIN_REPLICATION_H_ */
