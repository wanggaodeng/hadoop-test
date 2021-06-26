package com.wgd.customInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 17:40
 */
public class MyMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //文件名
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();
        context.write(new Text(name), value);
    }
}
