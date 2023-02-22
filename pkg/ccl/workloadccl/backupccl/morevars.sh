#!/bin/bash

# loads the config to introduce more variable required to run the scripts
. ./$1

let total_nodes=$crdb_nodes+1

# workload variables: should be set after config variables
if [[ "$workload" == "tpc-e" ]]; then
  init_cmd="sudo docker run cockroachdb/tpc-e:latest --init --customers=$workload_val --racks=$crdb_nodes \$(cat hosts.txt)"
  run_cmd="sudo docker run cockroachdb/tpc-e:latest --customers=$workload_val --racks=$crdb_nodes --duration=$duration \$(cat hosts.txt)"
elif [[ "$workload" == "tpc-c" ]]; then
  init_cmd="./workload init tpcc --data-loader import --warehouses $workload_val {pgurl:1-$crdb_nodes}" 
  run_cmd="./workload run tpcc --warehouses=$tpcc_run_val --duration=$duration --tolerate-errors {pgurl:1-$crdb_nodes}"
elif [[ "$workload" == "bank" ]]; then
  init_cmd="./workload init bank --data-loader import --rows=$workload_val --payload-bytes=$bank_payload_bytes {pgurl:1-$crdb_nodes}" 
  run_cmd="./workload run bank --payload-bytes=$bank_run_payload_bytes --duration=$duration --tolerate-errors --max-rate=$bank_run_max_rate {pgurl:1-$crdb_nodes}"
fi

# backup variables
prefix="gs"
if [[ $cloud == "aws" ]]; then
  prefix="s3"
fi
collection="$prefix://cockroach-fixtures/backups/$workload/$workload_var=$workload_val/$crdb_version/inc-count=$inc_count$auth"
