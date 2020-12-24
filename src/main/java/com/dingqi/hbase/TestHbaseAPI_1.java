package com.dingqi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbaseAPI_1 {

    public static Configuration conf;
    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop131");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void main(String[] args) throws IOException {

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        //获取操作对象


        Admin admin = connection.getAdmin();


        NamespaceDescriptor namespace = null;
        try {
            namespace = admin.getNamespaceDescriptor("dingqi");
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor build = NamespaceDescriptor.create("dingqi").build();
            admin.createNamespace(build);
        }

        TableName student = TableName.valueOf("dingqi:student");
        boolean flg = admin.tableExists(student);
        System.out.println(flg);
        if (flg) {

            Table table = connection.getTable(student);
            String rowkey = "1001";

            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            if(empty){
                Put put = new Put(Bytes.toBytes(rowkey));

                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));
                table.put(put);
            }else{
                for (Cell cell : result.rawCells()) {
                    System.out.println("value="+Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("family="+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("column="+Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }

        }else {
            HTableDescriptor hT = new HTableDescriptor(student);
            HColumnDescriptor cd = new HColumnDescriptor("info");
            hT.addFamily(cd);
            admin.createTable(hT);
            System.out.println("成功啦！");
        }
        //操作数据库

        //获取操作结果

        //关闭
    }
}
