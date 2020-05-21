package com.lei.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override // 统计每个手机的上行流量，下载流量，总流量
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sumUpflow = 0;
        long sumDownflow = 0;

        for (FlowBean v: values) {
            sumUpflow += v.getUpflow();
            sumDownflow += v.getDownflow();
        }

        // 封装对象
        FlowBean sumFlowbean = new FlowBean(sumUpflow, sumDownflow);

        // 写出
        context.write(key, sumFlowbean);

    }
}
