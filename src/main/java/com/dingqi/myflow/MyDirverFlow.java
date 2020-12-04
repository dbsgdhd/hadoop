package com.dingqi.myflow;

import com.dingqi.mywordcount.MyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyDirverFlow {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MyDirverFlow.class);


        job.setInputFormatClass(CombineTextInputFormat.class);


        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        job.setMapperClass(MuMapperFlow.class);
        job.setReducerClass(MyReducerFlow.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyFlowBean.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
