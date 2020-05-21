package com.lei.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// mapper输出对象是flowbean对象,包含upflow,downflow等等字段
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> { // 最重要: map方法

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override // 其实map的参数也可以看出，一开始拿到的KV就是<LongWritable, Text>类型
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 按 \t 切割
        String[] fields = line.split("\t");

        // 3 封装对象: 封装最终要输出的FlowBean对象
        k.set(fields[1]);
        v.setUpflow(Long.parseLong(fields[fields.length-3]));
        v.setDownflow(Long.parseLong(fields[fields.length-2]));
        v.setSumflow(Long.parseLong(fields[fields.length-1]));

        // 4 context 写出
        context.write(k, v);
    }

}
