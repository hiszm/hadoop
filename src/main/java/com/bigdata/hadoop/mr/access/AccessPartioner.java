package com.bigdata.hadoop.mr.access;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartioner extends Partitioner<Text, Access> {
    /**
     * @param phone 手机号
     */
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {
        if (phone.toString().startsWith("13")) {//13开头的
            return 0;
        } else if (phone.toString().startsWith("15")) {//15开头的
            return 1;
        } else {
            return 2;
        }
    }
}
