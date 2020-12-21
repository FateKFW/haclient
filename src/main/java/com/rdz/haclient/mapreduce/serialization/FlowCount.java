package com.rdz.haclient.mapreduce.serialization;

import com.rdz.haclient.mapreduce.serialization.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCount {
    public static void main(String[] args) {
        try {
            run(args);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取JOB对象
        Job job = Job.getInstance(new Configuration());

        // 设置jar包存放位置
        job.setJarByClass(FlowCount.class);

        // 关联Map和Reduce类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 设置Map阶段输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置最终数据输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置虚拟切片
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 切片20M
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
        job.setNumReduceTasks(2);

        // 提交job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}

class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean bean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 累加求和
        long upFlow = 0L, downFlow = 0L, sumFlow = 0L;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            sumFlow += value.getSumFlow();
        }
        // 写出，合并
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setSumFlow(sumFlow);
        context.write(key, bean);
    }
}

class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean v = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行
        String line = value.toString();

        // 按照空格划分
        String[] words = line.split("\t");

        // 封装对象，计算
        k.set(words[1]);
        v.setUpFlow(Long.valueOf(words[words.length - 3]));
        v.setDownFlow(Long.valueOf(words[words.length - 2]));
        v.calc();

        // 写出
        context.write(k, v);
    }
}