#!/bin/bash

run_test_on_3_nodes() {
  cmd=$1
  ssh centos@172.31.68.186 $cmd > out_1 2>&1 &
  FOO_PID_1=$!
  ssh centos@172.31.69.26 $cmd > out_2 2>&1 &
  FOO_PID_2=$!
  ssh centos@172.31.76.155 $cmd > out_3 2>&1 &
  FOO_PID_3=$!
  wait $FOO_PID_1
  wait $FOO_PID_2
  wait $FOO_PID_3
  cat out_1 && rm out_1
  cat out_2 && rm out_2
  cat out_3 && rm out_3
}

run_test_on_3_nodes "---------------THROUGHTPUT TEST--------------------" "cd raft; raft_dbg -c 512 -t 200 -r 5 -d 5 -x ~/raft-data -y \$RAFT_ID -l 3"
