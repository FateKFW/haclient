## 配置
core-site.xml  
> + io.compression.coders 压缩类全限定名 输入压缩

mapred-site.xml
> + mapreduce.map.output.compress false mapper输出
> + mapreduce.map.output.compress.codec 压缩类全限定名 mapper输出
> + mapreduce.output.fileoutputformat.compress false reducer输出
> + mapreduce.output.fileoutputformat.compress.codec 压缩类全限定名 reducer输出
> + mapreduce.output.fileoutputformat.compress.type RECORD reducer输出

## 代码
```java
    // 开启map端输出压缩
    configuration.setBoolean("mapreduce.map.output.compress", true);
    // 设置map端输出压缩方式
    configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

    // 设置reduce端输出压缩开启
    FileOutputFormat.setCompressOutput(job, true);
    // 设置压缩的方式
    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); 
```