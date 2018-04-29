package com.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author: * 相对那些封装好的方法而言的更底层一些的操作方式
 *            上层那些mapreduce spark等运算框架，去hdfs中获取数据的时候，就是调的这种底层的api
 * @description:
 * @date: 1:23 2018/4/22
 */
public class HdfsStreamTest {
    private FileSystem fileSystem;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), conf, "hadoop");
    }
    /**
     * @description: 通过流的方式上传文件
     * @date: 1:25 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void uploadFile() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/angelaByte.txt"));
        FileInputStream inputStream = new FileInputStream(new File("E:\\temp\\angelaBaby.txt"));
        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * @description: 通过流的方式下载文件
     * @date: 1:25 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void downFile() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/angelaByte.avi"));
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\angelaByte.avi"));
        IOUtils.copy(inputStream, outputStream);
    }

    /**
     * @description: 通过指定偏移量读取数据,用于上层数据处理并发框架处理
     * @date: 1:25 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void offsetFile() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/angelaByte.txt"));
        //对流的起始偏移量进行设置
        inputStream.seek(10L);
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\angelaBaby.txt"));
        IOUtils.copy(inputStream, outputStream);
        org.apache.hadoop.io.IOUtils.copyBytes(inputStream,outputStream,10L,true);
    }
}
