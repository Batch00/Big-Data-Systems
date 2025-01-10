# Use p4-hdfs as the base image
FROM p4-hdfs

# Command to start the DataNode
CMD hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000