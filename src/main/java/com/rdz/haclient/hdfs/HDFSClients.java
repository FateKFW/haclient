package com.rdz.haclient.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS客户端
 */
public class HDFSClients {
    private final static String URL = "hdfs://192.168.0.107:9000";

    public static void main(String[] args) {
        //mkdir("/user/project/test");
        //copyFromLocalFile(new Path("D:\\Study\\hadoop\\hadoop-2.7.2.tar.gz"), new Path("/user/project/test"));
        //copyToLocalFile(new Path("/user/project/test/hadoop-2.7.2.tar.gz"),new Path("D:\\Work"));
        fileDesc(new Path("/user/project/test"));
    }

    /**
     * 创建文件夹
     * @param path
     */
    public static void mkdir(String path) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            /*conf.set("fs.defaultFS", URI);
            //获取HDFS客户端对象
            fileSystem = FileSystem.get(conf);*/

            fileSystem = FileSystem.get(new URI(URL), conf, "rdz");

            //在HDFS上创建路径
            fileSystem.mkdirs(new Path(path));

            System.out.println("创建"+path+"成功");
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(fileSystem!=null)
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 将本地文件上传到HDFS
     * @param src
     * @param dst
     */
    public static void copyFromLocalFile(Path src, Path dst) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(URL), new Configuration(), "rdz");

            //上传
            fileSystem.copyFromLocalFile(src, dst);

            System.out.println(src.getName()+"文件上传成功");
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(fileSystem!=null)
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 下载HDFS集群中的文件
     * @param src
     * @param dst
     */
    public static void copyToLocalFile(Path src, Path dst) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(URL), new Configuration(), "rdz");

            //上传
            fileSystem.copyToLocalFile(src, dst);

            System.out.println(src.getName()+"文件下载成功，路径:"+dst.getParent());
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(fileSystem!=null)
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * 文件(夹)详情
     * @param src
     */
    public static void fileDesc(Path src) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(URL), new Configuration(), "rdz");

            System.out.println("*****类型*****");
            FileStatus[] fileStatuses = fileSystem.listStatus(src);
            for (FileStatus fs:fileStatuses) {
                System.out.println(fs.getPath().getName()+"是否为文件："+fs.isFile());
            }

            System.out.println("*****详情*****");
            RemoteIterator<LocatedFileStatus> locatedFileStatus = fileSystem.listFiles(src, true);
            while (locatedFileStatus.hasNext()) {
                LocatedFileStatus file = locatedFileStatus.next();

                // 文件名称
                System.out.println("文件名称:"+file.getPath().getName());
                // 长度
                System.out.println("长度:"+file.getLen());
                // 权限
                System.out.println("权限:"+file.getPermission());
                // 分组
                System.out.println("分组:"+file.getGroup());

                //块存储服务器节点
                BlockLocation[] blockLocations = file.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    // 获取块存储的主机节点
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }

            }

        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(fileSystem!=null)
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
}
