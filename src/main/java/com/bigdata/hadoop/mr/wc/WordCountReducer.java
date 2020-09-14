package com.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {


    /**
     * (hello,1)
     * @param key  单词
     * @param values 可以迭代的数量 例如world<1,1,1,1>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        Iterator<IntWritable> iterator  = values.iterator();
        while(iterator.hasNext()){
            //iterator.next();
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key,new IntWritable(count));

    }
}
