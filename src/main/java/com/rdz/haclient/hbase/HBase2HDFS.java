package com.rdz.haclient.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HBase数据转换到HDFS
 */
public class HBase2HDFS implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        //获取JOB
        Job job = Job.getInstance(configuration);
        //设置jar
        job.setJarByClass(HBase2HDFS.class);
        //设置
        TableMapReduceUtil.initTableMapperJob("t1", new Scan(), HBaseMapper.class, Text.class, NullWritable.class, job);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
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

    public static class HBaseMapper extends TableMapper<Text, NullWritable> {
        private Text k = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //1.创建字符串的对象
            StringBuffer sb = new StringBuffer();
            //2.获取扫描器
            CellScanner cellScanner = value.cellScanner();
            //3.扫描
            while (cellScanner.advance()) {
                //4.获取到表格
                Cell current = cellScanner.current();
                //5.拼接
                sb.append(new String(CellUtil.cloneValue(current))).append(",");
            }
            //6.设置k
            k.set(sb.toString());
            //7.写出
            context.write(k, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        //获取job
        ToolRunner.run(HBaseConfiguration.create(), new HBase2HDFS(), args);
    }
}