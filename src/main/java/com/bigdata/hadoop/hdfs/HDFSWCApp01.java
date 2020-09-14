package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HDFSWCApp01  {
    public static void main(String[] args) throws  Exception{

//1:读取HDFS上的文件
        Path input= new Path("/hdfsapi/local.txt");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop000:8020"),new Configuration(),"hadoop");

        //?迭代器
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);
        Mapper mapper =new WordCount();
        //创建上下文
        Context context= new Context();
        while(iterator.hasNext()){

            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line="";
            while ((line =reader.readLine())!=null){
 //2:词频统计
                //将业务逻辑处理完成后再返回給cache中
                mapper.map(line,context);
            }

            reader.close();
            in.close();

        }

 //3:将处理的结果混存起来 Map
        Map<Object,Object> contextMap = context.getCacheMap();
        //Map<Object,Object> contextMap = new HashMap<Object,Object>();
//4:将结果输出到HDFS
        Path output =new Path("/hdfsapi/output/");
        FSDataOutputStream out = fs.create(new Path(output,new Path("wc.out")));
        Set<Map.Entry<Object,Object>> entries = contextMap.entrySet();
        //迭代循环
        for(Map.Entry<Object,Object> entry : entries){
            out.write((entry.getKey().toString()+ "\t"+entry.getValue()+"\n").getBytes());

        }

        out.close();
        fs.close();
        System.out.println("统计完毕");


    }


}
