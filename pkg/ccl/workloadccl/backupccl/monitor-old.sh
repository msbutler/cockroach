#!/bin/bash

# This script checks the number of incremental backups that have run in a
# schedule every 5 minutes. If the number exceeds the predifined workload
# amount, the script will cancel the scheduled backup job.

. ./morevars.sh $1

roachprod list | grep $cluster_name
while :
do
  # sleep for 300 seconds, aka 5 mins
  sleep 300
  echo "Checking backup schedule"
  scheduleExists=$(roachprod sql $cluster_name:1 -- -e "SELECT label FROM [SHOW SCHEDULES] WHERE label ='schedule_cluster'")
  if [[ $scheduleExists == *"0 rows"* ]]; then
    echo "Schedule does not exist"
  fi

  backupCount=$(roachprod sql $cluster_name:1 -- -e "SELECT * FROM [SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN '$collection']] WHERE count < $inc_count")
  if [[ $backupCount == *"1 row"* ]]; then
    echo "$backupCount"
  elif [[ $backupCount == *"0 rows"* ]]; then
    echo "Cancelling schedule"
    roachprod sql $cluster_name:1 -- -e "DROP SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = 'schedule_cluster'"
    exit 0
  fi

done
