package com.lei.mapreduce.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"/Users/lei/Desktop/大数据/尚硅谷hadoop/input/inputphone",
                "/Users/lei/Desktop/大数据/尚硅谷hadoop/output/outputphone"};
        // 1 获取job对象
        Configuration conf  = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 set current jar class
        job.setJarByClass(FlowCountDriver.class);

        // 3 关联mapper, reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 4 mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 5 输出结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 输入输出路径
        FileInputFormat.setInputPaths(job,
                new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 job配置的参数，和job所用的java类所在的jar包，提交给yarn运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
