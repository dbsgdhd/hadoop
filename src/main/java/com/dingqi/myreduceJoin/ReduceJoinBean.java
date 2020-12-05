package com.dingqi.myreduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceJoinBean implements Writable{

    private String order_id;
    private String p_id;
    private int amount;
    private  String pnmae;
//    private string

    public ReduceJoinBean() {
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPnmae() {
        return pnmae;
    }

    public void setPnmae(String pnmae) {
        this.pnmae = pnmae;
    }

    @Override
    public String toString() {
        return order_id +"\n"+ p_id +"\n"+ amount +"\n"+ pnmae ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pnmae);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pnmae  = in.readUTF();

    }
}
