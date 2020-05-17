# a small part

# datanodes (using default slaves file)

if [ -n "$HADOOP_SECURE_DN_USER" ]; then
  echo \
    "Attempting to start secure cluster, skipping datanodes. " \
    "Run start-secure-dns.sh as root to complete startup."
else
  "$HADOOP_PREFIX/sbin/hadoop-daemons.sh" \
    --config "$HADOOP_CONF_DIR" \
    --script "$bin/hdfs" start datanode $dataStartOpt
fi
