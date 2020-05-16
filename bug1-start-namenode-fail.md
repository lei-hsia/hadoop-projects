desc: 

`sbin/hadoop-daemon.sh start namenode`; `jps` shows no namenode process;

possible: 

1. `hadoop namenode format`: executed when there are datanodes alive. All NN,DN should be killed before format namenode
