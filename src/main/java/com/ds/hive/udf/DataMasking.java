package com.ds.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 身份证号手机号加密
 * */
public class DataMasking extends UDF {
    public String evaluate(String s) {
        String outPut = "";
        String startString = "";
        String endString = "";
        if (s.length() == 11) {
            startString = s.substring(0, 3);
            endString = s.substring(7, 11);
            outPut = startString + "****" + endString;
        } else if (s.length() == 8) {
            startString = s.substring(0, 3);
            endString = s.substring(6, 8);
            outPut = startString + "***" + endString;
        } else if (s.length() == 18) {
            startString = s.substring(0, 6);
            endString = s.substring(14, 18);
            outPut = startString + "********" + endString;
        } else {
            outPut = s;
        }
        return outPut;
    }
}
