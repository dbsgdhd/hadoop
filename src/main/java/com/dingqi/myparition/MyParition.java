package com.dingqi.myparition;

import com.dingqi.myflow.MyFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyParition extends Partitioner<MyFlowBean,Text> {

//
//    @Override
//    public int getPartition(Text text, MyFlowBean myFlowBean, int numPartitions) {
//        String substring = text.toString().substring(0, 3);
//        int count = 4;
//        switch(substring){
//            case "136":
//                count=0;
//                break;
//
//            case "137":
//                count=1;
//                break;
//
//            case "138":
//                count=2;
//                break;
//
//            case "139":
//                count=3;
//                break;
//        }
//        return  count;
//    }

    @Override
    public int getPartition(MyFlowBean myFlowBean, Text text, int numPartitions) {
        String substring = text.toString().substring(0, 3);
        int count = 4;
        switch(substring){
            case "136":
                count=0;
                break;

            case "137":
                count=1;
                break;

            case "138":
                count=2;
                break;

            case "139":
                count=3;
                break;
        }
        return  count;
    }
}
