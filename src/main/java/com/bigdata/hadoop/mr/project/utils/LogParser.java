package com.bigdata.hadoop.mr.project.utils;

import com.sun.istack.internal.NotNull;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    public Map<String, String> parse(String log) {
        IPParser ipParser = IPParser.getInstance();
        Map<String, String> info = new HashMap<>();

        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");

            String ip = splits[13];
            String country = "-";
            String province = "-";
            String city = "-";

            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            if (regionInfo != null) {
                country = regionInfo.getCountry();
                province = regionInfo.getProvince();
                city = regionInfo.getCity();
            }

            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[1];
            info.put("url", url);

            String time = splits[17];
            info.put("time", time);
            String[] client1 = splits[29].split("/");;
            String client = client1[0];
            info.put("client", client);
        }

        return info;
    }

    public Map<String, String> parseV2(String log) {
        IPParser ipParser = IPParser.getInstance();
        Map<String, String> info = new HashMap<>();

        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\t");

            String ip = splits[0];
            String country = splits[1] ;
            String province = splits[2];
            if(province.equals("null")){
                province="其它";
            }
            String city = splits[3];
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            info.put("ip", ip);
            info.put("country", country);
            info.put("province", province);
            info.put("city", city);

            String url = splits[4];
            info.put("url", url);

            String time = splits[5];
            info.put("time", time);

            String client = splits[6];
            info.put("client", client);
        }

        return info;
    }
}
