# get job manager and hdfs name node:
export HDFS_NAME_NODE="$(curl http://169.254.169.254/latest/meta-data/public-hostname):8020"
export FLINK_JOB_MANAGER=$(curl "$(yarn application -list |  grep RUNNING | grep Flink | cut -f9)/jobmanager/config" | jd -r 'from_entries | ."jobmanager.rpc.address"')
export FLINK_JOB_MANAGER_PORT=$(curl "$(yarn application -list |  grep RUNNING | grep Flink | cut -f9)/jobmanager/config" | jd -r 'from_entries | ."jobmanager.rpc.port"')
echo -e "cluster setup:\n hdfs: $HDFS_NAME_NODE\n flink: $FLINK_JOB_MANAGER:$FLINK_JOB_MANAGER_PORT"

