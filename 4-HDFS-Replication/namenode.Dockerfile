# Use p4-hdfs as the base image
FROM p4-hdfs

# Command to start the NameNode
CMD hdfs namenode -format && hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -fs hdfs://boss:9000