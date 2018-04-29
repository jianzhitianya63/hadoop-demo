package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * @author: ganduo
 * @description:
 * @date: 23:20 2018/4/21
 */
public class HdfsClientTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        fileSystem = FileSystem.get(new URI("hdfs://node1:9000"), conf, "hadoop");
    }

    /**
     * @description:上传文件
     * @date: 0:18 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void uploadFile() throws IOException {
        fileSystem.copyFromLocalFile(
                new Path("C:\\WiFi_Log.txt")
                , new Path("/abc"));
        fileSystem.close();
    }

    /**
     * @description:从HDFS复制文件
     * @date: 0:18 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void fromHDFScopyToLocal() throws Exception {
       fileSystem.copyToLocalFile(true,new Path("/abc/WiFi_Log.txt"), new Path("e:/"),true);
       fileSystem.close();
    }

    /**
     * @description:删除 创建 重命名HDFS目录
     * @date: 0:18 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void deleteOrCreateOrResetFileNmae() throws Exception {
        //创建多级目录,相对路径为/user/hadoop
        fileSystem.create(new Path("/b"));
        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        //fileSystem.delete(new Path("/abc"),true);
        //重命名文件或文件名
        //fileSystem.rename(new Path("/abc/WiFi_Log.txt"), new Path("/abc/angela.txt"));
    }

    /**
     * @description: 查看文件信息,只显示文件
     * @date: 0:18 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void showFileInfo() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            //文件名
            System.out.println(file.getPath().getName());
            //块大小
            System.out.println(file.getBlockSize());
            //文件大小
            System.out.println(file.getLen());
            //用户
            System.out.println(file.getOwner());
            //权限
            System.out.println(file.getPermission());
            System.out.println("-----------------------------------");
            //块信息
            BlockLocation[] blockLocations = file.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
            System.out.println("-----------------------------------");
        }
    }

    /**
     * @description:查看文件夹
     * @date: 0:18 2018/4/22
     * @param
     * @return:
     */
    @Test
    public void selectDir() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses) {
            //是否是文件
            if (!fileStatus.isFile()) {
                System.out.println(fileStatus.getPath().getName());
            }
        }
    }
}
