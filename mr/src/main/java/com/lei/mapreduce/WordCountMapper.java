package com.lei.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // atguigu atguigu --> <atguigu,1> <atguigu,1>

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 输出
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        // 1. 获取一行 (kv: offset, value) value本身设计就是按行输入
        String line = value.toString();
        
        // 2. 切割一行
        String[] words = line.split(" ");
        
        // 3. 
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
