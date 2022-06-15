package com.ds.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
public class HelloUDF extends UDF{
    public String evaluate(String str){
        try{
            return "helloWorld" + str;
        }catch (Exception e){
            return null;
        }
    }
}