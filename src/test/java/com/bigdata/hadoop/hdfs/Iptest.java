package com.bigdata.hadoop.hdfs;

import com.bigdata.hadoop.mr.project.utils.IPParser;
import org.junit.Test;

public class Iptest {
    @Test
    public void testIP(){
        IPParser.RegionInfo regionInfo =IPParser.getInstance().analyseIp("58.32.19.255");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());

    }
}
