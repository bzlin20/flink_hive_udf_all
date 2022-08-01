package com.ds.hive.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName SchemaHive
 * @Description hive 取最大分区
 * @Author ds-longju
 * @Date 2022/8/1 7:54 下午
 * @Version 1.0
 **/
public class SchemaHive  extends UDF {
    public String evaluate(String str) {
        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList();
        String ss = null;
        String newStr = str.toString().replace("'", "").replace("\"", "").replace("=", "");
        System.out.println(newStr);
        String split1 = newStr.split("\\.")[0];
        String split2 = newStr.split("\\.")[1];
        // hive 表存储地址
        String fileName = sb.append("/dtInsight/hive/warehouse/").append(split1).append(".db/").append(split2).toString();
        System.out.println(fileName);

        try{
            ss = getFileList(fileName);
        }catch (Exception e){
            System.out.println("分区异常" +e.getMessage());
        }
        return ss;
    }

    public static String getFileList(String path) throws Exception{
        String res = null;

        Configuration conf=new Configuration(false);
        String nameservices = "ns1";
        String[] namenodesAddr = {"172.16.10.177:9000","172.16.10.175:9000"};
        String[] namenodes = {"nn1","nn2"};

        // fs.defaultFS：hdfs://ns1
        conf.set("fs.defaultFS", "hdfs://" + nameservices);
        // dfs.nameservices：ns1
        conf.set("dfs.nameservices",nameservices);
        //dfs.ha.namenodes.ns1：nn1,nn2
        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
        // dfs.namenode.rpc-address.ns1.nn2：172.16.10.177:9000
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
        //dfs.namenode.rpc-address.ns1.nn1：172.16.10.175:9000
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
        conf.set("dfs.client.failover.proxy.provider." + nameservices
                ,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        String hdfsRPCUrl = "hdfs://" + nameservices + ":" + 8020;

        FileSystem hdfs = FileSystem.get(URI.create(path),conf);
        FileStatus[] fs = hdfs.listStatus(new Path(path));
        Path[] listPath = FileUtil.stat2Paths(fs);

        List<String> lists = new ArrayList();
        for(Path p : listPath){
// hdfs://ns1/dtInsight/hive/warehouse/itg.db/ads_wrapper_goods_open_90d_above_1w/pt=20220720
            String partition = p.toString().split("=")[1];
            try {
                lists.add(partition);
            }catch (Exception e){
                System.out.println("异常数据，" + partition);
            }
        }

        return Collections.max(lists).toString();
    }
}
