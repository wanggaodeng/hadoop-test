package com.wgd.keyvalueInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: keyvalueInputFormat
 * @date 2021/6/26 16:54
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {
    LongWritable outValue = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,outValue);
    }
}
