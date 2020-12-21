package com.rdz.haclient.mapreduce.custompartitioner;

import com.rdz.haclient.mapreduce.serialization.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区(结果分文件)
 */
/*
// 8 指定自定义数据分区
job.setPartitionerClass(ProvincePartitioner.class);
// 9 同时指定相应数量的reduce task
job.setNumReduceTasks(5);
*/
public class MyPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        return 0;
    }
}
