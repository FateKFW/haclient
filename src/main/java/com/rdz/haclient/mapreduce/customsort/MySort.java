package com.rdz.haclient.mapreduce.customsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 将Pojo作为key，mr任务中key会自动调用排序
 */
public class MySort implements WritableComparable<MySort> {
    private long num1;
    private long num2;

    @Override
    public int compareTo(MySort o) {
        return (int) (this.num1 - o.getNum1());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(num1);
        dataOutput.writeLong(num2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num1 = dataInput.readLong();
        num2 = dataInput.readLong();
    }

    public long getNum1() {
        return num1;
    }
}
