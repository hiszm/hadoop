package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN    : Map任务读数据的key类型,offset,是每行数据起始位置的偏移量,Long
 * VALUEIN  : Map任务读数据的value,其实就是一行行的字符串
 * KEYOUT   : Map方法自定义输出的key的类型 String
 * VALUEOUT : Map方法自定义输出的value的类型 Integer
 * 这些是java的自定义的类型的
 *
 * hadoop自定义类型: 序列化和反序列化
 * LongWriteable, Text
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把value按照指定的分割符分开
        String[] words=value.toString().split("\t");
        for(String word: words){
            context.write(new Text(word.toLowerCase()),new IntWritable(1));
        }
    }
}
