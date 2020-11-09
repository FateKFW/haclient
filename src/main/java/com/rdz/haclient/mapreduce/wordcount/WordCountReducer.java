package com.rdz.haclient.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN,VALUEIN map阶段输出的kv对
 * KEYOUT,VALUEOUT reducer输出
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 累加输出
        int sum = 0;
        for (IntWritable value:values) {
            sum += value.get();
        }

        // 写出
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}
