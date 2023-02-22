#!/bin/bash

# This script can be used to setup a roachprod cluster, init and run a workload 
# and create backup fixture. I'd recommend the following workflow:
# - run script once to setup the cluster and init the workload
# - wait until the tmux session monitoring initizialation has finished
# - run the script again to setup the backup schedule and the workload, 
#   skipping setup
# 
# To run the script, pass the config file name when executing this script. For
# example, `./fixture.sh tpce8TB`
set -e

. ./morevars.sh $1

if [[ "$2" == "help" ]]; then
  echo "This script makes it a bit easier to update backup figures. 

Run any of the following commands via:
./fixture.sh [config_name] [cmd]

Available commands:
- setup: creates a roachprod cluster.
- init: inits the workload, via bulk import.
- run: runs the workload.
- backup: begins a backup schedule that writes to the cockroach-fixtures.
  bucket. If $cloud is aws, write s3, else gs.
- monitor: creates a tmux session on a roachprod node that cancels the backup schedule once the
  target number of incremental backups are written. This can only be used if the backup was
  created with implicit-auth, as we don't want to put cloud credentials on roachprod clusters.
- monitor-old: is the same as monitor, but conducts the monitoring locally and can be used to
  monitor backups that used explicit auth, if the proper environment variables have been set in
  with the $auth parameter).
"

echo "config from $1:
hardware: 
  crdb_nodes: $crdb_nodes 
  total_nodes: $total_nodes
  cloud: $cloud
  machine_type: $machine_type
  vol_size: $vol_size GBs
crdb:
  cluster_name: $cluster_name
  crdb_version: $crdb_version
workload: $workload 
  $workload_var: $workload_val
  duration: $duration
  init_cmd: $init_cmd
  run_cmd: $run_cmd
backup:
  inc_count: $inc_count
  inc_crontab: $inc_crontab
  auth: $auth
  collection: $collection
"
  exit 0
fi



if [[ "$2" == "setup" ]]; then 
    
    if [[ "$cloud" == "gce" ]]; then
    	roachprod create $cluster_name -c gce --nodes=$total_nodes --gce-machine-type=$machine_type --local-ssd=false --gce-pd-volume-size=$vol_size --gce-min-cpu-platform='Intel Ice Lake'
    elif [[ "$cloud" == "aws" ]]; then
	roachprod create $cluster_name -c aws --nodes=$total_nodes --aws-machine-type=$machine_type --local-ssd=false --aws-ebs-volume-size=$vol_size
    else
	echo "only aws and gce clouds are supported"
  	exit 1  
  fi
    roachprod install $cluster_name:$total_nodes docker
    roachprod ip  $cluster_name:1-$crdb_nodes | sed 's/^/--hosts=/' | tr '\n' ' ' > hosts.txt
    roachprod put $cluster_name:$total_nodes hosts.txt
    rm hosts.txt
    roachprod stage $cluster_name:1-$crdb_nodes release $crdb_version
    roachprod stage $cluster_name:$total_nodes workload --os linux
    roachprod start $cluster_name:1-$crdb_nodes --racks=$crdb_nodes
    roachprod extend $cluster_name -l 48h
    echo "roachprod cluster $cluster_name  is set up"
fi

if [[ "$2" == "init" ]]; then
  if [[ "$workload" == "tpc-e" ]]; then
    #without this check, we silently init the very small "fixed" fixture.  
    exists=$(gsutil ls gs://cockroach-fixtures/tpce-csv/customers=$workload_val | wc -l)
    if (( $exists == 0 )); then
      echo "$tpce_customers customer fixture does not exist"
      exit 1
    fi
  fi

  roachprod run $cluster_name:$total_nodes -- "tmux new -d -s import \"$init_cmd\""
  echo "Success!
  init has begun. Do not run anything on cluster until
  after init process completes. To observe init script, run: 
  roachprod ssh $cluster_name:$total_nodes
and attach to the tmux session:
  tmux attach-session -t import "
  exit 0
fi

if [[ "$2" == "run" ]]; then
  roachprod run $cluster_name:$total_nodes -- "tmux new -d -s run \"$run_cmd\""
  echo "Workload has began running! To observe progress, run:
  roachprod ssh $cluster_name:$total_nodes
and look at the run.log:
  tmux attach_session -t run "
fi

if [[ "$2" == "backup" ]]; then

  if [[ "$override_latest" == "false" ]]; then
    declare backupExists=($(roachprod sql $cluster_name:1 -- -e "SHOW BACKUPS FROM '$collection'"))
    if [[ ${backupExists[1]} != "" ]]; then
      echo "A backup already exists in this collection. By default, the user can not create a new
       backup in a non-empty collection. To override and to understand the implications, see the
        override_latest parameter."
      exit 1
      fi
  fi
  # This backup schedule will first run a full backup immediately and then the
  # incremental backups at the given crontab cadence until the user cancels the
  # backup schedules. To ensure that only one full backup chain gets created,
  # begin the backup schedule at the beginning of the week, as a new full
  # backup will get created on Sunday at Midnight ;)
  #
  schedule_cmd="CREATE SCHEDULE schedule_cluster FOR BACKUP INTO '$collection' WITH revision_history RECURRING '$inc_crontab' FULL BACKUP '@weekly' WITH SCHEDULE OPTIONS first_run = 'now'"
  echo "$schedule_cmd"
  roachprod sql $cluster_name:1 -- -e "$schedule_cmd"
fi

if [[ "$2" == "monitor" ]]; then
  roachprod put $cluster_name:1 "conf_default" "conf_default"
  roachprod put $cluster_name:1 "monitor.sh" "monitor.sh"
  roachprod put $cluster_name:1 "morevars.sh" "morevars.sh"
  roachprod put $cluster_name:1 "$1" "$1"
  roachprod run $cluster_name:1 -- "tmux new -d -s monitor \"./monitor.sh $1\"" 
  echo "Monitor script is setup! After $inc_count backups have been created, the script will stop the backup schedule.
to watch progress, run
  roachprod ssh $cluster_name:1
and either inspect the log at monitor.log or attach to the tmux session
  tmux attach-session -t monitor"
fi

if [[ "$2" == "monitor-old" ]]; then
  echo "About to start a tmux session. Make sure nothing can kill it!"
  echo "Pro tip: run this on your gce worker after calling sudo touch /.active"
  tmux new -d -s monitor "./monitor-old.sh $1 > monitor.log"
fi
