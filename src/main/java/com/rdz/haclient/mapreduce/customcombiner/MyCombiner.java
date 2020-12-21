package com.rdz.haclient.mapreduce.customcombiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 使用：job.setCombinerClass
 */
public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable longWritable : values) {
            sum += longWritable.get();
        }

        v.set(sum);

        context.write(key, v);
    }
}
