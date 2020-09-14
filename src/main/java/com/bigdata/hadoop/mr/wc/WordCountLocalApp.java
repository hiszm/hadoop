package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//使用MR统计HDFS上的文件对应的词频
//Driver: 配置Mapper,Reducer的相关属性
public class WordCountLocalApp {
    public static void main(String[] args) throws Exception{




        Configuration configuration= new Configuration();
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
        //设置job对应的参数:作业输出的的路径
        FileInputFormat.setInputPaths(job,new Path("input"));
        FileOutputFormat.setOutputPath(job,new Path("output"));

        //提交job
        boolean result=job.waitForCompletion(true);

        System.exit( result ? 0:-1);



    }
}
