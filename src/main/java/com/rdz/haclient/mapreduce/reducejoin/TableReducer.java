package com.rdz.haclient.mapreduce.reducejoin;

import com.rdz.haclient.mapreduce.reducejoin.pojo.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orders = new ArrayList<>();
        TableBean pd = new TableBean();

        for (TableBean bean : values) {
            if ("order".equals(bean.getFlag())) {
                TableBean tmp = new TableBean();
                try {
                    BeanUtils.copyProperties(tmp, bean);
                    orders.add(tmp);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pd, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean bean : orders) {
            bean.setPname(pd.getPname());
            context.write(bean, NullWritable.get());
        }
    }
}
