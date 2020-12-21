package com.rdz.haclient.mapreduce.reducejoin.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    // 订单ID
    private String id;
    // 产品ID
    private String pid;
    // 数量
    private int num;
    // 产品名称
    private String pname;
    // 标记位（订单表or产品表）
    private String flag;

    public TableBean() {
    }

    public TableBean(String id, String pid, int num, String pname, String flag) {
        this.id = id;
        this.pid = pid;
        this.num = num;
        this.pname = pname;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 序列化
        output.writeUTF(id);
        output.writeUTF(pid);
        output.writeInt(num);
        output.writeUTF(pname);
        output.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // 反序列化
        this.id = input.readUTF();
        this.pid = input.readUTF();
        this.num = input.readInt();
        this.pname = input.readUTF();
        this.flag = input.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "id='" + id + "\'\t" +
                ", pid='" + pid + "\'\t" +
                ", num=" + num +
                ", pname='" + pname + "\'\t" +
                ", flag='" + flag + "\'";
    }
}
