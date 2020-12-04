package com.dingqi.myflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducerFlow extends Reducer<Text,MyFlowBean,Text,MyFlowBean> {

    private MyFlowBean myFlowBean = new MyFlowBean();
    @Override
    protected void reduce(Text key, Iterable<MyFlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (MyFlowBean value : values) {
            sum_downFlow+=value.getDownlong();
            sum_upFlow+=value.getUplong();
        }
        myFlowBean.set(sum_upFlow, sum_downFlow);
        context.write(key, myFlowBean);

    }
}
