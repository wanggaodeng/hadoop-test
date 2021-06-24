package com.hdfs.operate;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wang
 * @version 1.0
 * @description: TODO
 * @date 2021/6/24 21:43
 */
public class HDFSOperate {
    @Test
    public void mkDirOnHDFS() throws IOException {
        //配置项
        Configuration configuration = new Configuration();
        //设置要连接的hdfs集群
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        //获得文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //调用方法创建目录；若目录已经存在，则创建失败，返回false
        boolean mkdirs = fileSystem.mkdirs(new Path("/wgd/dir1"));
        //释放资源
        fileSystem.close();
    }
    //指定目录所属用户
    @Test
    public void mkDirOnHDFS2() throws IOException, URISyntaxException, InterruptedException {
        //配置项
        Configuration configuration = new Configuration();
        //获得文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "test");
        //调用方法创建目录
        boolean mkdirs = fileSystem.mkdirs(new Path("/wgd/dir2"));
        //释放资源
        fileSystem.close();
    }
    //创建目录时，指定目录权限
    @Test
    public void mkDirOnHDFS3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        FileSystem fileSystem = FileSystem.get(configuration);
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);
        boolean mkdirs = fileSystem.mkdirs(new Path("hdfs://node01:8020/wgd/dir3"), fsPermission);
        if (mkdirs) {
            System.out.println("目录创建成功");
        }

        fileSystem.close();
    }
    /**
     * 说明：
     * 将文件hello.txt上传到/wgd/dir1
     * 如果路径/wgd/dir1不存在，那么结果是：
     * 在hdfs上先创建/wgd目录
     * 然后，将hello.txt上传到/wgd中
     * 如果路径/wgd/dir1存在，那么将hello.txt上传到此路径中去
     *
     * @throws IOException
     */
    @Test
    public void uploadFile2HDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("G:\\work\\kkb\\HDFS课件\\3、数据\\hello.txt"),
                new Path("/wgd/dir1"));//hdfs路径
        fileSystem.close();
    }

    @Test
    public void downloadFileFromHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/wgd/dir1/hello.txt"), new Path("G:\\mydata\\hello.txt"));
        //删除文件
        //fileSystem.delete();
        //重命名文件
        //fileSystem.rename()
        fileSystem.close();
    }

    /**
     * 获取文件详情
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void viewFileInfo() throws IOException, InterruptedException, URISyntaxException {
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("hdfs://node01:8020/wgd/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        // 3 关闭资源
        fs.close();
    }

    /**
     * 通过io流进行数据上传操作
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2 创建输入流 不需要加file:///，否则报错
        FileInputStream fis = new FileInputStream(new File("G:\\mydata\\hello.txt"));
        // 3 获取输出流 父目录不存在，会自动创建
        FSDataOutputStream fos = fs.create(new Path("hdfs://node01:8020/wgd/dir3/hello.txt"));
        // 4 流对拷 org.apache.commons.io.IOUtils
        IOUtils.copy(fis, fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fs.close();
    }

    /**
     * 通过io流进行数据下载
     */
    @Test
    public void downloadFileFromHDFSByIO() throws IOException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 2 创建输入流 不需要加file:///，否则报错
        FileOutputStream fos = new FileOutputStream(new File("G:\\mydata\\hello2.txt"));
        // 3 获取输出流 父目录不存在，会自动创建
        FSDataInputStream fis = fs.open(new Path("hdfs://node01:8020/wgd/dir3/hello.txt"));
        // 4 流对拷 org.apache.commons.io.IOUtils
        IOUtils.copy(fis, fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fs.close();
    }
    /**
     * 小文件合并：读取所有本地小文件，写入到hdfs的大文件里面去
     */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        //获取分布式文件系统hdfs
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "hadoop");

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://node01:8020/wgd/bigfile.xml"));

        //获取本地文件系统 localFileSystem
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //读取本地的文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("G:\\work\\kkb\\HDFS课件\\3、数据\\smallfile"));
        for (FileStatus fileStatus : fileStatuses) {
            //获取每一个本地的文件路径
            Path path = fileStatus.getPath();
            //读取本地小文件
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);

            IOUtils.copy(fsDataInputStream, fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
        }

        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
    }

}
