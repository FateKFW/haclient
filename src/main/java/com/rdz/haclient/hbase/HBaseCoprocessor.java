package com.rdz.haclient.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

/**
 * 协处理器是HBase中一种高级组件，可以让用户在region所在的服务器上运行自定义代码，与传统的RDBMS中的触发器和存储过程有点类似。
 * 分为：
 * observer -> 触发器
 * endpoint -> 存储过程
 *
 * 部署：
 * 打包jar
 * 上传到HDFS
 * 执行命令导入协处理器
 *      disable 'teacher'
 *      alter 'teacher',METHOD => 'table_att', 'coprocessor' => 'hdfs://localhost:9000/hbase/XXX.jar|HBaseCoprocessor|1001'
 *      enable 'teacher'
 *
 */
public class HBaseCoprocessor extends BaseRegionObserver {

    @Autowired
    private HBaseClient hBaseClient;

    /*
    teacher
        COLUMN FAMILIES DESCRIPTION
        {NAME => 'student', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL =
        > 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
    student
        COLUMN FAMILIES DESCRIPTION
        {NAME => 'teacher', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL =
        > 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
     */

    /**
     * 在Put命令执行之前调用
     * @param e
     * @param put           put命令执行的数据
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取行键
        byte[] rowKey = put.getRow();
        //获取到name属性对应的多个版本的值
        List<Cell> cells = put.get("student".getBytes(), "name".getBytes());
        //获取最新版本
        Cell cell = cells.get(0);
        //获取值
        byte[] value = CellUtil.cloneValue(cell);

        //创建新的Put
        Put newPut = new Put(value);
        newPut.addColumn("teacher".getBytes(), "name".getBytes(), rowKey);
        //提交
        Table table = hBaseClient.getTable("student");
        table.put(put);
        hBaseClient.closeTable(table);
    }
}