package com.rdz.haclient.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    private static String inFile = "D:\\MyTools\\MapReduce\\data.txt";
    private static String outFile = "D:\\MyTools\\MapReduce\\compressOutput\\result";
    private static String unOutFile = "D:\\MyTools\\MapReduce\\compressOutput\\result.txt";

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 测试压缩
        // compress(inFile, "org.apache.hadoop.io.compress.BZip2Codec");

        // 解压缩
        uncompress(outFile + ".bz2");
    }

    private static void compress(String path, String clazz) throws IOException, ClassNotFoundException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(path));
        CompressionCodec compress =
                (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), new Configuration());

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(
                new File(outFile + compress.getDefaultExtension()));
        CompressionOutputStream outputStream = compress.createOutputStream(fos);

        // 流的对拷
        IOUtils.copyBytes(fis, outputStream, 1024 * 1024, false);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    private static void uncompress(String path) throws IOException {
        // 合法性检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(path));
        if (codec == null) {
            System.out.println("无法处理");
        }

        // 输入输出流
        try (FileInputStream fis = new FileInputStream(new File(path));
             CompressionInputStream cis = codec.createInputStream(fis);
             FileOutputStream fos = new FileOutputStream(new File(unOutFile))) {
            // 流的对拷
            IOUtils.copyBytes(cis, fos, 1024 * 1024, false);
        }
    }
}
