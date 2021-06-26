package com.wgd.customInputFormat;

import com.wgd.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 17:43
 */
public class MyInputFormatMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(new Configuration(), new MyInputFormatMain(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "customInputFormat");
        //如果要集群运行，需要加
        job.setJarByClass(MyInputFormatMain.class);

        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //没有reduce。但是要设置reduce的输出的k3   value3 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //将我们的文件输出成为SequenceFile这种格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}
