package com.wgd.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 12:52
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(new Configuration(), new WordCount(), args);
        //根据我们 程序的退出状态码，退出整个进程
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        /***
         * 第一步：读取文件，解析成key,value对，k1   v1
         * 第二步：自定义map逻辑，接受k1   v1  转换成为新的k2   v2输出
         * 第三步：分区。相同key的数据发送到同一个reduce里面去，key合并，value形成一个集合
         * 第四步：排序   对key2进行排序。字典顺序排序
         * 第五步：规约  combiner过程  调优步骤 可选
         * 第六步：分组
         * 第七步：自定义reduce逻辑接受k2   v2  转换成为新的k3   v3输出
         * 第八步：输出k3  v3 进行保存
         */
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "wordcount");
        //在本地运行可以不添加
        //实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        //如果要打包到集群上面去运行，必须添加以下设置
        job.setJarByClass(WordCount.class);
        //第一步：读取文件，解析成key,value对，k1:行偏移量  v1：一行文本内容
        job.setInputFormatClass(TextInputFormat.class);
        //指定我们去哪一个路径读取文件
        TextInputFormat.addInputPath(job, new Path(args[0]));
        //第二步 自定义map逻辑，接受k1   v1  转换成为新的k2   v2输出
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //第3分区、4分区内排序、5combine、6分组

        //第7 reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第8 outputFormat 输出k3  v3 进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        //一定要注意，输出路径是需要不存在的，如果存在就报错
        //判断输出路径，是否存在，如果存在，则删除
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        // reduce task 数量 本地运行无效
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        //提交job任务
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}
