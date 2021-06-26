package com.wgd.secondsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 19:20
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {

    private FlowSortBean flowSortBean;

    //初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 手机号	上行包	下行包	上行总流量	下行总流量
         * 13480253104	3	3	180	180
         */
        String[] fields = value.toString().split("\t");

        flowSortBean.setPhone(fields[0]);
        flowSortBean.setUpPackNum(Integer.parseInt(fields[1]));
        flowSortBean.setDownPackNum(Integer.parseInt(fields[2]));
        flowSortBean.setUpPayload(Integer.parseInt(fields[3]));
        flowSortBean.setDownPayload(Integer.parseInt(fields[4]));

        context.write(flowSortBean, NullWritable.get());
    }
}