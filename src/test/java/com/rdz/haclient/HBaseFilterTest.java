package com.rdz.haclient;

import com.rdz.haclient.hbase.HBaseClient;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * HBase过滤器查询
 */
@SpringBootTest
public class HBaseFilterTest {
    @Autowired
    private HBaseClient hBaseClient;

    /**
     * 单列过滤器
     */
    @Test
    void testSingle() {
        //创建单列值过滤器
        SingleColumnValueFilter single =
                new SingleColumnValueFilter("f1".getBytes(), "age".getBytes(), CompareFilter.CompareOp.LESS, "22".getBytes());
        //设置单值过滤，没有这个属性的不再查出
        single.setFilterIfMissing(true);

        //创建Scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(single);
        //获取表
        Table t1 = hBaseClient.getTable("t1");
        //打印查询结果
        hBaseClient.showScanner(t1, scan);
        hBaseClient.closeTable(t1);
    }

    /**
     * 过滤器链
     */
    @Test
    void testFilterChain() {
        //过滤器链
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //年龄过滤器
        SingleColumnValueFilter age =
                new SingleColumnValueFilter("f1".getBytes(), "age".getBytes(), CompareFilter.CompareOp.LESS, "22".getBytes());
        age.setFilterIfMissing(true);
        //名字过滤器
        SingleColumnValueFilter name =
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zs".getBytes());
        name.setFilterIfMissing(true);

        //添加到过滤器链
        list.addFilter(age);
        list.addFilter(name);

        //创建Scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(list);
        //获取表
        Table t1 = hBaseClient.getTable("t1");
        //打印查询结果
        hBaseClient.showScanner(t1, scan);
        hBaseClient.closeTable(t1);
    }

    /**
     * 正则比较器
     */
    @Test
    void testRegular() {
        //正则比较器
        RegexStringComparator regex = new RegexStringComparator("s+");

        //创建单列值过滤器
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, regex);
        //设置单值过滤，没有这个属性的不再查出
        filter.setFilterIfMissing(true);

        //打印结果
        hBaseClient.showFilter("t1", filter);
    }
}