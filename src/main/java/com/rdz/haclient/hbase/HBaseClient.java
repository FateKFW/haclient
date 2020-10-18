package com.rdz.haclient.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;

@Component
public class HBaseClient {
    private static String configIP;

    @Value("${hbase.ip}")
    public void setConfigIP(String configIP) {
        HBaseClient.configIP = configIP;
    }

    /**
     * 获取HBaseAdmin<br/>
     * 从配置文件中读取hbase地址
     * @return
     */
    public HBaseAdmin getAdmin() {
        HBaseAdmin admin = null;
        try {
            admin = getAdmin0(this.configIP);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 获取HBaseAdmin<br/>
     * @param ip hbase所在IP
     * @return
     */
    public HBaseAdmin getAdmin(String ip) {
        HBaseAdmin admin = null;
        try {
            admin = getAdmin0(ip);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 关闭HBaseAdmin
     * @param admin
     */
    public void closeHBaseAdmin(HBaseAdmin admin) {
        try {
            closeHBaseAdmin0(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取表<br/>
     * 从配置文件中读取hbase地址
     * @param tableName 表名
     * @return
     */
    public Table getTable(String tableName) {
        try {
            return getTable0(configIP, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取表
     * @param ip        hbase所在IP
     * @param tableName 表名
     * @return
     */
    public Table getTable(String ip, String tableName) {
        try {
            return getTable0(ip, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭Table
     * @param table
     */
    public void closeTable(Table table) {
        try {
            closeTable0(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void showScanner(Table table, Scan scan) {
        try {
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                CellScanner cellScanner = result.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    //行键
                    System.out.print(new String(CellUtil.cloneRow(cell), "utf-8") + " ");
                    //列簇
                    System.out.print(new String(CellUtil.cloneFamily(cell), "utf-8") + ":");
                    //列名
                    System.out.print(new String(CellUtil.cloneQualifier(cell), "utf-8") + " = ");
                    //列值
                    System.out.println(new String(CellUtil.cloneValue(cell), "utf-8"));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void showFilter(String tableName, Filter filter) {
        //创建Scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filter);
        //执行查询
        Table table = getTable(tableName);
        try {
            table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //打印查询结果
        showScanner(table, scan);
        //关闭表
        closeTable(table);
    }

    /**
     * 获取表
     * @param ip
     * @param tableName
     * @return
     * @throws IOException
     */
    private Table getTable0(String ip, String tableName) throws IOException {
        if(StringUtils.isEmpty(tableName)) return null;

        //FIXME:获取Connection方式优化,此处用于演示
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", ip);
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }

    /**
     * 获取HBaseAdmin
     * @param ip
     * @return
     * @throws IOException
     */
    private HBaseAdmin getAdmin0(String ip) throws IOException {
        //FIXME:获取Connection方式优化,此处用于演示
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", ip);
        HBaseAdmin admin = (HBaseAdmin) ConnectionFactory.createConnection(configuration).getAdmin();
        return admin;
    }

    /**
     * 关闭HBaseAdmin
     * @param admin
     * @throws IOException
     */
    private void closeHBaseAdmin0(HBaseAdmin admin) throws IOException {
        if(admin!=null)
            admin.close();
    }

    /**
     * 关闭Table
     * @param table
     */
    private void closeTable0(Table table) throws IOException {
        if (table != null)
            table.close();
    }
}
