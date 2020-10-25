package com.rdz.haclient.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * HDFS数据转换到HBase
 */
public class HDFS2Hbase implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        //获取JOB
        Job job = Job.getInstance(configuration);
        //设置
        job.setJarByClass(HDFS2Hbase.class);
        job.setMapperClass(HDFSMapper.class);
        job.setReducerClass(HBaseReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("t3", HBaseReducer.class, job);
        //提交
        return job.waitForCompletion(true) ? 1: 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("hbase.zookeeper.quorum", "myserver");
        configuration.set("fs.defaultFS", "hdfs://myserver:9000");
        configuration.set("mapreduce.framework.name", "yarn");
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static class HDFSMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text k = new Text();
        private LongWritable v = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取一行
            String line = value.toString();
            //切
            String[] cols = line.split(",");
            //遍历
            for (int i = 0; i < cols.length; i++) {
                k.set(cols[i]);
                context.write(k, v);
            }
        }
    }

    public static class HBaseReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                LongWritable next = iterator.next();
                count += next.get();
            }
            Put put = new Put(key.toString().getBytes());
            put.addColumn("t1".getBytes(), "name".getBytes(), key.toString().getBytes());
            put.addColumn("t1".getBytes(), "count".getBytes(), Bytes.toBytes(count));
            context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
        }
    }

    public static void main(String[] args) throws Exception {
        //获取job
        ToolRunner.run(HBaseConfiguration.create(), new HBase2HDFS(), args);
    }
}
