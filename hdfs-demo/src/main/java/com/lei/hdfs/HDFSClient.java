package com.lei.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();
        // 访问哪个集群，设置配置信息
        //conf.set("fs.defaultFS", "hdfs://hadoop100:9000");  // Permission denied: user=xialei ..

        // 1 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.get(new URI("hdfs://hadoop100:9000"), conf, "lei");

        // 2 hdfs上创建路径  mkdirs(Path path)
        fileSystem.mkdirs(new Path("/0517/hdfs-shen"));

        // 3 close
        fileSystem.close();

        System.out.println("over");


    }
}
