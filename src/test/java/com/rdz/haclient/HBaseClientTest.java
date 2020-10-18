package com.rdz.haclient;

import com.rdz.haclient.hbase.HBaseClient;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Iterator;

/**
 * HBase基础操作
 */
@SpringBootTest
public class HBaseClientTest {
    @Autowired
    private HBaseClient hBaseClient;

    //namespace

    /**
     * 表存在
     * @throws IOException
     */
    @Test
    void testExists() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();
        boolean t1 = admin.tableExists("t1");
        System.out.println("表t1存在：" + t1);
        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 创建命名空间
     * @throws IOException
     */
    @Test
    void testCreateNamespace() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("ns1").build();
        admin.createNamespace(descriptor);
        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 列出所有命名空间
     * @throws IOException
     */
    @Test
    void testListNamespace() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor ns:namespaceDescriptors) {
            System.out.println(ns);
        }
        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 列出命名空间下所有表明细
     * @throws IOException
     */
    @Test
    void testListNamespaceTables() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();
        TableName[] defaults = admin.listTableNamesByNamespace("default");
        for (TableName ta:defaults) {
            System.out.println(ta);
        }
        hBaseClient.closeHBaseAdmin(admin);
    }

    //DDL

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    void testCreateTable() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();

        //表描述
        TableName name = TableName.valueOf("t2");
        HTableDescriptor table = new HTableDescriptor(name);
        //列簇
        table.addFamily(new HColumnDescriptor("name"));
        table.addFamily(new HColumnDescriptor("age"));

        admin.createTable(table);

        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 修改表
     * @throws IOException
     */
    @Test
    void testAlterTable() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();

        //表描述
        TableName tableName = TableName.valueOf("t1");
        HTableDescriptor t1 = admin.getTableDescriptor(tableName);
        //列簇
        HColumnDescriptor name = new HColumnDescriptor("name");
        name.setVersions(1, 5);
        t1.addFamily(name);

        admin.modifyTable(tableName, t1);

        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 删除列簇
     * @throws IOException
     */
    @Test
    void testDeleteColumn() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();
        admin.deleteColumn("t1", "age");
        hBaseClient.closeHBaseAdmin(admin);

        //第二种删除方式
        /*
        TableName tableName = TableName.valueOf("t1");
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        tableDescriptor.removeFamily("age".getBytes());
        admin.modifyTable(tableName, tableDescriptor);
        */
    }

    /**
     * 列出所有列簇
     * @throws IOException
     */
    @Test
    void testListColumn() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();

        TableName tableName = TableName.valueOf("t1");
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);

        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor col:columnFamilies) {
            System.out.println(col);
        }
        hBaseClient.closeHBaseAdmin(admin);
    }

    /**
     * 删除表
     * @throws IOException
     */
    @Test
    void testDropTable() throws IOException {
        HBaseAdmin admin = hBaseClient.getAdmin();

        TableName tableName = TableName.valueOf("t1");
        if(admin.tableExists(tableName)) {
            if(admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
        hBaseClient.closeHBaseAdmin(admin);
    }

    //DML

    @Test
    void testPut() throws IOException {
        Table table = hBaseClient.getTable("t1");

        Put put = new Put("1001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "ls".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), "20".getBytes());
        table.put(put);

        hBaseClient.closeTable(table);
    }

    @Test
    void testGet() throws IOException {
        Table table = hBaseClient.getTable("t1");

        Get get = new Get("1000".getBytes());
        Result result = table.get(get);
        //第一种
        /*
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("f1".getBytes());
        for (Map.Entry<byte[], byte[]> entry:familyMap.entrySet()) {
            System.out.println(new String(entry.getKey())+" "+new String(entry.getValue()));
        }
        */

        //第二种
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            //列簇
            System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8") + ":");
            //列名
            System.out.print(new String(CellUtil.cloneQualifier(cell), "utf-8") + " = ");
            //列值
            System.out.println(new String(CellUtil.cloneValue(cell), "utf-8"));
        }


        hBaseClient.closeTable(table);
    }

    @Test
    void testScan() throws IOException {
        Table table = hBaseClient.getTable("t1");

        Scan scan = new Scan();
        scan.withStartRow("1000".getBytes());
        scan.withStopRow("1002".getBytes());
        scan.addColumn("f1".getBytes(), "name".getBytes());

        //结果扫描器
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            System.out.println("================");
            Result result = iterator.next();

            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                //列簇
                System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8") + ":");
                //列名
                System.out.print(new String(CellUtil.cloneQualifier(cell), "utf-8") + " = ");
                //列值
                System.out.println(new String(CellUtil.cloneValue(cell), "utf-8"));
            }
        }
        hBaseClient.closeTable(table);
    }

    @Test
    void testDelete() throws IOException {
        Table table = hBaseClient.getTable("t1");

        Delete delete = new Delete("1002".getBytes());
        table.delete(delete);

        hBaseClient.closeTable(table);
    }
}