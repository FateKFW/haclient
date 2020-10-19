package com.rdz.haclient.hbase.filter;

import com.rdz.haclient.hbase.HBaseClient;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
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
     * where name='zs'
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
     * where age<22 and name='zs'
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
     * where name like '%z%'
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

    /**
     * 子串比较器
     * where name like '%z%'
     */
    @Test
    void testSubstring() {
        //子串比较器
        SubstringComparator comparator = new SubstringComparator("z");

        //创建单列值过滤器
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        //设置单值过滤，没有这个属性的不再查出
        filter.setFilterIfMissing(true);

        //打印结果
        hBaseClient.showFilter("t1", filter);
    }

    /**
     * 二进制比较器
     * 单列比较器就是使用二进制比较器
     * where name = 'zs'
     */
    @Test
    void testBinary() {
        //二进制比较器
        BinaryComparator comparator = new BinaryComparator("zs".getBytes());

        //创建单列值过滤器
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        //设置单值过滤，没有这个属性的不再查出
        filter.setFilterIfMissing(true);

        //打印结果
        hBaseClient.showFilter("t1", filter);
    }

    /**
     * 二进制前缀比较器
     * where name like 'zs%'
     */
    @Test
    void testPreBinary() {
        //二进制前缀比较器
        BinaryPrefixComparator comparator = new BinaryPrefixComparator("zs".getBytes());

        //创建单列值过滤器
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        //设置单值过滤，没有这个属性的不再查出
        filter.setFilterIfMissing(true);

        //打印结果
        hBaseClient.showFilter("t1", filter);
    }

    /**
     * 列簇过滤器
     * 列簇 like '%f2%'
     */
    @Test
    void testFamilyFilter() {
        //正则比较器
        RegexStringComparator comparator = new RegexStringComparator("f2");
        //创建单列值过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, comparator);
        //打印结果
        hBaseClient.showFilter("t1", familyFilter);
    }

    /**
     * 列簇过滤器2
     * 列簇 like '%f%'
     */
    @Test
    void testFamilyFilter2() {
        SubstringComparator comparator = new SubstringComparator("f");
        //创建单列值过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, comparator);
        //打印结果
        hBaseClient.showFilter("t1", familyFilter);
    }

    /**
     * 列名过滤器
     * 列名 like '%na%'
     */
    @Test
    void testColumnName() {
        SubstringComparator comparator = new SubstringComparator("na");
        //列名过滤器
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, comparator);
        //打印结果
        hBaseClient.showFilter("t1", qualifierFilter);
    }

    /**
     * 列名前缀过滤器
     */
    @Test
    void testPreColumnName() {
        //列名 like 'na%'
        //ColumnPrefixFilter filter = new ColumnPrefixFilter("na".getBytes());
        //hBaseClient.showFilter("t1", filter);

        //列名 like 'na%' or 列名 like 'a%'
        byte [][] pres = {"na".getBytes(), "a".getBytes()};
        MultipleColumnPrefixFilter mfilter = new MultipleColumnPrefixFilter(pres);
        hBaseClient.showFilter("t1", mfilter);
    }

    /**
     * 列范围过滤器
     * 查询列名在XXX到YYY的数据
     */
    @Test
    void testColumnRange() {
        ColumnRangeFilter filter = new ColumnRangeFilter("age".getBytes(), true, "name".getBytes(), false);
        hBaseClient.showFilter("t1", filter);
    }

    /**
     * 行键过滤器
     * rowkey = '1000'
     */
    @Test
    void testRowKey() {
        BinaryComparator comparator = new BinaryComparator("1000".getBytes());
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, comparator);
        hBaseClient.showFilter("t1", filter);
    }

    /**
     * 只拿每行的第一个列
     */
    @Test
    void testFirstKey() {
        hBaseClient.showFilter("t1", new FirstKeyOnlyFilter());
    }
}