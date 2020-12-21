package com.rdz.haclient.mapreduce.customout;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * job.setOutputFormatClass
 */
public class MyOutput extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MyRecordWriter(job);
    }
}

class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsDataOutputStream1;
    FSDataOutputStream fsDataOutputStream2;

    public MyRecordWriter() {
    }

    public MyRecordWriter(TaskAttemptContext job) {
        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // 创建文件流
            fsDataOutputStream1 = fs.create(new Path("D:/my/jishu.txt"));
            fsDataOutputStream2 = fs.create(new Path("D:/my/oushu.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断逻辑
        int price = Integer.parseInt(key.toString().split(" ")[2]);
        if ((price & 1) == 1) {
            fsDataOutputStream1.write(key.toString().getBytes());
        } else {
            fsDataOutputStream2.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fsDataOutputStream1);
        IOUtils.closeStream(fsDataOutputStream2);
    }
}