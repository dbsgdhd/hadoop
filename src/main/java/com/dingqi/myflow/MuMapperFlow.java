package com.dingqi.myflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MuMapperFlow extends Mapper<LongWritable,Text,MyFlowBean,Text> {
    Text numphone = new Text();
    MyFlowBean values = new MyFlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phone  = split[1];
        numphone.set(phone);
        long up = Long.parseLong(split[split.length - 3]);
        long dowm = Long.parseLong(split[split.length - 2]);
        values.set(up,dowm);
        context.write(values,numphone);
    }
}
