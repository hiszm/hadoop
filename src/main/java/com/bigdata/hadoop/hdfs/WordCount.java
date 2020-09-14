package com.bigdata.hadoop.hdfs;

public class WordCount implements Mapper{
    @Override
    public void map(String line, Context context) {
        String[] words = line.split("\t");
        for(String word: words){
            Object value = context.get(word);

            if(value==null){//没有出现该单词
                context.write(word,1);
            }else{//已经有了,取出value再+1
                int v =Integer.parseInt(value.toString());
                context.write(word,v+1);
            }
        }
    }
}
