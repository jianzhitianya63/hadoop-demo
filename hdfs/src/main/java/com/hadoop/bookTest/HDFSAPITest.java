package com.hadoop.bookTest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Arrays;


/**
 * @author: ganduo
 * @description: Hadoop权威指南Demo
 * @date: 13:53 2018/4/29
 */
public class HDFSAPITest {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://node1:9000/"), configuration, "hadoop");
    }
    @Test
    public void FileSystemCreate() throws IOException {
        FSDataOutputStream fsOut = null;
        try {
            fsOut = fs.create(new Path("/user/tom/quangle.txt"));
            fsOut.write("On the top of the Crimpetty Tree".getBytes());
        } finally {
            fsOut.close();
        }
    }
    /**
     * @description: 指定偏移量读取, seek()消耗量大
     * @date: 14:33 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void fileSystemDoubleCat() throws IOException {
        FSDataInputStream fsIn = null;
        try {
            fsIn = fs.open(new Path("/user/tom/quangle.txt"));
            fsIn.seek(5);
            IOUtils.copyBytes(fsIn, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsIn);
        }
    }

    @Test
    public void fileSystemDoubleCat2() throws IOException {
        FSDataInputStream fsIn = null;
        try {
            fsIn = fs.open(new Path("/user/tom/quangle.txt"));
            byte[] bytes = new byte[1024];
            fsIn.read(5, bytes, 5, 5);
            System.out.println(Arrays.toString(bytes));
        } finally {
            IOUtils.closeStream(fsIn);
        }
    }

    /**
     * @description: 复制本地文件到HDFS, 打印进度
     * @date: 14:27 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void fileCopyWithProgress() throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(new File("E:\\log.txt")));
        FSDataOutputStream fsOut = fs.create(new Path("/user/tom/log.txt"), new Progressable() {
            @Override
            public void progress() {
                System.out.print("=");
            }
        });
        IOUtils.copyBytes(bufferedInputStream, fsOut, 4096, true);
    }

    /**
     * @description: 显示Hadoop文件系统中一组路径的文件信息
     * @date: 14:27 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void listStatus() throws IOException {
        Path[] paths = new Path[2];
        paths[0] = new Path("/user");
        paths[1] = new Path("/user/tom");
        FileStatus[] fileStatuses = fs.listStatus(paths);
        Path[] resultFilePathInfo = FileUtil.stat2Paths(fileStatuses);
        for (Path filepathInfo:
             resultFilePathInfo) {
            System.out.println(filepathInfo);
        }
    }

    /**
     * @description: 使用正则表达式和文件过滤器查找文件路径信息
     * @date: 14:27 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void patternSelectFile() throws IOException {
        FileStatus[] fileStatuses = fs.globStatus(new Path("/*"), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if (path.toString().contains("user")) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        Path[] paths = FileUtil.stat2Paths(fileStatuses);
        for (Path path:
             paths) {
            System.out.println(path);
        }
    }

    /**
     * @description: 删除文件
     * @date: 14:27 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void deleteFile() throws IOException {
        try {
            fs.delete(new Path("/user/"), false);
        } catch (IOException e) {
            System.out.println("非空目录不允许删除,除非第二个参数为true");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 使用hflush()或hsync()刷新数据到内存或硬盘,当我们再写入一个块时,这个块对其他eader是不可见的,这是hadoop为了性能
     *               做的取舍,我们可使用hflush将数据刷新到内存,或使用hsync()刷新到硬盘,再一点就是out.close()操作隐含了hflush(),
     *               如果数据要求对数据的一致性非常高,那么我们就要在性能和数据完整性中做取舍,因为如果没有使用hsync()那么如果断电将丢失
     *               正在写的数据,如果没有使用hflush那么在写入数据还没有close时宕机,将丢失正在写的数据
     * @date: 14:27 2018/4/29
     * @param
     * @return:
     */
    @Test
    public void flushAndFsync() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("user/tom/flush.txt"));
        fsDataOutputStream.write("content".getBytes());
        // fsDataOutputStream.flush();
        fsDataOutputStream.hsync();
        fsDataOutputStream.close();
    }
}
