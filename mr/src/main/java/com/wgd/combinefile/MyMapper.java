package com.wgd.combinefile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: 词频统计-map
 * @date 2021/6/26 12:36
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取当前行数据
        String line = value.toString();
        // 获取一个个的单词
        String[] words = line.split(",");
        for(String word : words){
            // 将kv对输出出去
            context.write(new Text(word),new IntWritable(1));
            //将每个单词出现都记做1次
            //key2  Text类型
            //v2  IntWritable类型
           /* text.set(word);
            //将我们的key2  v2写出去到下游
            context.write(text,intWritable);*/
        }
    }
}
