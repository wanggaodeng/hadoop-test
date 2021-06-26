package com.wgd.keyvalueInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 16:58
 */
public class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long result = 0;
        for (LongWritable value : values) {
            long l = value.get();
            result += l;
        }

        context.write(key, new LongWritable(result));
    }
}
