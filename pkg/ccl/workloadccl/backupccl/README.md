# Backup Fixture Generator Tools

`fixture.sh` provides a suite of cmds to easily spin up a roachprod cluster,
initialize and run a workload (tpce, tpcc, and bank), and run scheduled backups
for a specified number of incremental backups. The shell scripts in this
directory do not connect at all with the `workload` cli. The script aims to
speed up the backup fixture generation process, making it easier to create and
maintain our restore roachtest suite, and conduct ad hoc large scale testing. 

To get a sense of the available commands and to view the target backup
collection URI, run `./fixture conf_default help`.

Each `conf_*` script defines the configurations for an actively maintained
backup fixture. The default fixture is `conf_default`, and all other conf files
inherit their default values from `conf_default`.

All other shell scripts in the directory are invoked by `fixture.sh`.

To run all commands in this script, you must have:
- the roachprod cli [set up](https://cockroachlabs.atlassian.net/wiki/spaces/TE/pages/144408811/Roachprod+Tutorial). Specifically, ensure the `roachprod` 
  cmd is in your path. 
- working aws cli and gsutil cli installations.
- set the `CLUSTER` environment variable to your default roachprod cluster name.
