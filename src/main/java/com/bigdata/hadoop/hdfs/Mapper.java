package com.bigdata.hadoop.hdfs;

public interface Mapper {
    /**
     * 自定义上下文
     * @param line 读取到每一行数据
     * @param context 上下文/缓存
     */
    public void map(String line, Context context);
}
