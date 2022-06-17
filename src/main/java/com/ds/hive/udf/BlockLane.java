package com.ds.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * @author  longju
 * @date 202220616
 * */
public class BlockLane  extends UDF {
    public String evaluate(String s1){
        String xx = s1.substring(0, 1);
        String berthLane = "";
        String yy = CheDao(s1);
        berthLane = xx + "#" + yy;
        return berthLane;
    }
    public static String CheDao(String s1) {
        String yy = s1.substring(1, 2);
        String zz = "";
        if (yy.equals("1")) {
            zz = "11";
        } else if (yy.equals("2")) {
            zz = "23";
        } else if (yy.equals("3")) {
            zz = "23";
        } else if (yy.equals("4")) {
            zz = "45";
        } else if (yy.equals("5")) {
            zz = "45";
        } else if (yy.equals("6")) {
            zz = "67";
        } else if (yy.equals("7")) {
            zz = "67";
        } else if (yy.equals("8")) {
            zz = "89";
        } else if (yy.equals("9")) {
            zz = "89";
        } else if (yy.equals("A")) {
            zz = "AB";
        } else if (yy.equals("B")) {
            zz = "AB";
        } else if (yy.equals("C")) {
            zz = "CD";
        } else if (yy.equals("D")) {
            zz = "CD";
        } else if (yy.equals("E")) {
            zz = "EE";
        } else if (yy.equals("F")) {
            zz = "FG";
        } else if (yy.equals("G")) {
            zz = "FG";
        } else if (yy.equals("H")) {
            zz = "HK";
        } else if (yy.equals("K")) {
            zz = "HK";
        }
        return zz;
    }
}
