package com.rdz.haclient.mapreduce.reducejoin;

import com.rdz.haclient.mapreduce.reducejoin.pojo.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text k = new Text();
    private TableBean v = new TableBean();
    private String name;

    @Override
    protected void setup(Context context) {
        // 获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String[] contents = value.toString().split("\t");
        if (name.startsWith("order")) {
            // 订单表
            v.setId(contents[0]);
            v.setPid(contents[1]);
            v.setNum(Integer.parseInt(contents[2]));
            v.setPname("");
            v.setFlag("order");

        } else {
            // 产品表
            v.setId("");
            v.setPid(contents[0]);
            v.setNum(0);
            v.setPname(contents[1]);
            v.setFlag("pd");
        }

        k.set(v.getPid());

        context.write(k, v);
    }
}
