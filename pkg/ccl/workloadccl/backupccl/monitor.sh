#!/bin/bash

# This script checks the number of incremental backups that have run in a
# schedule every 5 minutes. If the number exceeds the predifined workload
# amount, the script will cancel the scheduled backup job.

. ./morevars.sh $1

while :
do
  sleep 300
  echo "Checking backup schedule"
  declare scheduleExists=($(./cockroach sql --insecure -e "SELECT label FROM [SHOW SCHEDULES] WHERE label ='schedule_cluster'"))
  if [[ ${scheduleExists[1]} == "" ]]; then
    echo "Schedule does not exist"
    continue
  fi
  
  declare inc_running=($(./cockroach sql --insecure -e "SELECT count(*) FROM [SHOW SCHEDULES] WHERE label ='schedule_cluster' and schedule_status='ACTIVE'"))
  if (( ${inc_running[1]} < 2 )); then
    # implies the incremental schedule has not started 
    continue 
  fi

  declare backupCount=($(./cockroach sql --insecure -e "SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN '$collection']"))
  echo "Backup count ${backupCount[1]}"
  if (( ${backupCount[1]} > $inc_count )); then
    echo "Cancelling schedule"
    ./cockroach sql --insecure -e "DROP SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = 'schedule_cluster'"
    exit 0
  fi

done
