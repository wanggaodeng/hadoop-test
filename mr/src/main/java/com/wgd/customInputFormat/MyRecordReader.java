package com.wgd.customInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/26 17:22
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    //要读取的分片
    private FileSplit fileSplit;
    private Configuration configuration;
    //当前的value值
    private BytesWritable bytesWritable;

    //标记一下分片有没有被读取；默认是false
    private boolean flag = false;

    /**
     * 初始化
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
        this.bytesWritable = new BytesWritable();
    }

    /**
     *  RecordReader读取分片时，先判断是否有下一个kv对，根据flag判断；
     *  如果有，则一次性的将文件内容全部读出
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!flag){
            long length = fileSplit.getLength();
            byte[] splitContent = new byte[(int) length];
            //读取分片内容
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);

            //split内容写入splitContent
            IOUtils.readFully(inputStream, splitContent, 0, (int) length);
            //当前value值
            bytesWritable.set(splitContent, 0, (int) length);
            flag = true;

            IOUtils.closeStream(inputStream);
            //fileSystem.close();

            return true;
        }
        return false;
    }

    /**
     * 获取当前键值对的键
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取当前键值对的值
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 读取分片的进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag ? 1.0f : 0.0f;
    }

    /**
     * 释放资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
