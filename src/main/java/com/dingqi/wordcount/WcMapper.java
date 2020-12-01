package com.dingqi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private  Text word= new Text();
    private  IntWritable one = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //拿到一行元素
        String line = value.toString();

        //切分成数组
        String[] words = line.split(" ");

        //便利数组，把单词变成（word,1）的形式交给框架

        for (String s:words) {
            this.word .set(s);
            context.write(this.word, this.one);
        }

    }
}
