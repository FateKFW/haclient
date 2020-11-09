package com.rdz.haclient.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// KEYIN 输入数据的key
// VALUEIN 输入数据的value
// KEYOUT 输出数据的类型
// VALUEOUT 输出数据的value
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String line = value.toString();

        // 按照空格切分数据
        String[] words = line.split(" ");

        // 循环写出
        for (String word:words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
