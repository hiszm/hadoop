package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.net.URI;

//使用MR统计HDFS上的文件对应的词频
//Driver: 配置Mapper,Reducer的相关属性
public class WordCountApp {
    public static void main(String[] args) throws Exception{


        System.setProperty("HADOOP_USER_NAME","hadoop");
        //System.setProperty("hadoop.home.dir", "/home/hadoop/app/hadoop-2.6.0-cdh5.15.1");

        Configuration configuration= new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.1.200:8020");
        //创建一个job
        Job job = Job.getInstance(configuration);
        //设置job对应的参数:主类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Job对应的参数:Mapper输出Key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置job对应的参数:Reducer输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果路径存在就删除
        FileSystem fs=FileSystem.get(new URI("hdfs://192.168.1.200:8020"),configuration,"hadoop");
        Path output=new Path("/map/outpu");

        if(fs.exists(output)){
            fs.delete(output,true);
        }

        //设置job对应的参数:作业输出的的路径
        FileInputFormat.setInputPaths(job,new Path("/map/input"));
        FileOutputFormat.setOutputPath(job,new Path("output"));

        //提交job
        boolean result=job.waitForCompletion(true);

        System.exit( result ? 0:-1);



    }
}
