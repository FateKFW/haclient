package com.rdz.haclient.mapreduce.custominput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split, context);

        return recordReader;
    }
}

class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit split;
    private Configuration configuration;
    private Text key;
    private BytesWritable value;
    private Boolean progress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
        this.key = new Text();
        this.value = new BytesWritable();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 核心业务逻辑

        if (progress) {
            // 获取fs对象
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);

            // 获取输入流
            FSDataInputStream fsdis = fileSystem.open(path);

            // 拷贝
            byte[] buff = new byte[(int) split.getLength()];
            IOUtils.readFully(fsdis, buff, 0, buff.length);

            // 封装value
            value.set(buff, 0, buff.length);

            // 封装key
            key.set(path.toString());

            // 关闭资源
            IOUtils.closeStream(fsdis);

            progress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void close() {

    }
}
