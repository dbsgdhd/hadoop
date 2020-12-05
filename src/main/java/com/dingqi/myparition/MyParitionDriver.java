package com.dingqi.myparition;

import com.dingqi.myflow.MuMapperFlow;
import com.dingqi.myflow.MyDirverFlow;
import com.dingqi.myflow.MyFlowBean;
import com.dingqi.myflow.MyReducerFlow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyParitionDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MyParitionDriver.class);

        job.setMapperClass(MuMapperFlow.class);
        job.setReducerClass(MyReducerFlow.class);

        job.setMapOutputKeyClass(MyFlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFlowBean.class);

        job.setPartitionerClass(MyParition.class);

        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b =job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
