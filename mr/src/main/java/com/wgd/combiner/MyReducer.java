package com.wgd.combiner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 12:45
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value : values){
            int count = value.get();
            sum += count;
        }
        context.write(key,new IntWritable(sum));
    }
}
