package com.lei.mapreduce.flowsum;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class FlowBean implements Writable {

    private long upflow;
    private long downflow;
    private long sumflow;

    public FlowBean(){
        super();
    }

    public FlowBean(long upflow, long downflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        sumflow = upflow+downflow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException { // 接收
        upflow = dataInput.readLong();
        downflow = dataInput.readLong();
        sumflow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow;
    }

    public void set(long upflow, long downflow) {
        // 一个set方法设置所有的三个fields
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
    }
}
