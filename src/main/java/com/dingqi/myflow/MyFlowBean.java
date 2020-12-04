package com.dingqi.myflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyFlowBean implements Writable {


    private Long uplong ;
    private Long downlong ;
    private Long sum ;

    public MyFlowBean() {
    }

    public void set(Long uplong,Long downlong) {
        this.downlong=downlong;
        this.uplong= uplong;

        this.sum = uplong + downlong;
    }

    @Override
    public String toString() {
        return  uplong +"\t" + downlong + "\t" + sum ;
    }


    public Long getUplong() {
        return uplong;
    }

    public void setUplong(Long uplong) {
        this.uplong = uplong;
    }

    public Long getDownlong() {
        return downlong;
    }

    public void setDownlong(Long downlong) {
        this.downlong = downlong;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
            out.writeLong(uplong);
            out.writeLong(downlong);
            out.writeLong(sum);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uplong = in.readLong();
        this.downlong = in.readLong();
        this.sum = in.readLong();
    }
}
