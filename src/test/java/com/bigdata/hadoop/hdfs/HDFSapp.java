package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

//使用java API来操作文件系统
public class HDFSapp {

    public static final String HDFS_PATH="hdfs://hadoop000:8020";
    FileSystem fileSystem=null;
    Configuration configuration=null;


    @Before
    public void setUp() throws Exception{
        System.out.println("=======setUp=======");
        //fileSyetem参数(指定uri,客户端指定的配置参数,客户端的身份即用户名)
        configuration=new Configuration();
        configuration.set("dfs.replication","1");
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }

    //创建文件夹
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test2"));
    }
    //查看HDFS内容
    @Test
    public void text() throws Exception{
        FSDataInputStream in=fileSystem.open(new Path("/README.txt"));
        IOUtils.copyBytes(in,System.out,1024);

    }
    //========================================
    //创建文件
    @Test
    public void create() throws Exception{
        FSDataOutputStream out=fileSystem.create(new Path("/hdfsapi/b.txt"));
        out.writeUTF("hello world replication");
        out.flush();
        out.close();
    }
    //重命名
    @Test
    public void rename() throws Exception{
        Path oldPath=new Path("/hdfsapi/a.txt");
        Path newPath=new Path("/hdfsapi/c.txt");
        boolean result =fileSystem.rename(oldPath,newPath);
        System.out.println(result);
    }
    //上传本地文件到HDFS系统
    @Test
    public void copyFromLocalFile() throws Exception{
        Path src =new Path("/Users/jacksun/local.txt");
        Path dst =new Path("/hdfsapi/");
        fileSystem.copyFromLocalFile(src,dst);
    }

    //上传本地文件到HDFS系统(大文件,进度条)
    @Test
    public void copyFromLocalBigFile() throws Exception{
        InputStream in =new BufferedInputStream(new FileInputStream(new File("/Users/jacksun/data/music.ape")));
        FSDataOutputStream out =fileSystem.create(new Path("/hdfsapi/music.ape"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("-");
                    }
                }
        );
        IOUtils.copyBytes(in,out,1024);
    }

    // 从HDFS系统下载文件到本地
    @Test
    public void copyToLocalFile() throws Exception{
        Path src =new Path("/hdfsapi/hello.txt");
        Path dst =new Path("/Users/jacksun/data");
        fileSystem.copyFromLocalFile(src,dst);

    }

    //列出文件列表
    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi"));
        for (FileStatus file : statuses) {
            printFileStatus(file);
        }
    }

    private void printFileStatus(FileStatus file) {
        String isDir = file.isDirectory() ? "文件夹" : "文件";
        String permission = file.getPermission().toString();
        short replication = file.getReplication();
        long length = file.getLen();
        String path = file.getPath().toString();

        System.out.println(isDir + "\t" + permission + "\t" +
                replication + "\t" + length + "\t" + path);
    }
    //递归列出所有文件
    @Test
    public void listFileRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(
                new Path("/hdfsapi"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            printFileStatus(file);
        }
    }




     // 查看文件块信息
    @Test
    public void getFielBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(
                new Path("/hdfsapi/test/a.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,
                0, fileStatus.getLen());

        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + ":" + block.getOffset() + ":" +
                        block.getLength() + ":" + block.getHosts());
            }
        }
    }

    //删除文件
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/local.txt"), true);
    }


    @After
    public void tearDown(){
        configuration=null;
        fileSystem=null;
        System.out.println("=======tearDown=======");

    }


//    public static void main(String[] args) throws Exception {
//        Configuration configuration =new Configuration();
//        FileSystem fileSystem= FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");
//        Path path= new Path("/hdfsapi/test");
//        boolean result= fileSystem.mkdirs(path);
//        System.out.println(result);
//    }

}
