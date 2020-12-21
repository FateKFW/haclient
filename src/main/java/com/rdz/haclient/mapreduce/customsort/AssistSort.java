package com.rdz.haclient.mapreduce.customsort;

import org.apache.hadoop.io.WritableComparator;

/**
 * job.setGroupingComparatorClass
 */
public class AssistSort extends WritableComparator {

    protected AssistSort() {
        super(MySort.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        MySort aa = (MySort) a;
        MySort bb = (MySort) b;
        // 要求只要ID相同就认为是相同的key，那么就会放到一个reduce去
        int result = (int) (aa.getNum1() - bb.getNum1());
        return result;
    }
}
