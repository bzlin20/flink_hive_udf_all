package com.ds.test;

public class stringBufferTest {
    public static class StringAccum {
        public StringBuffer total = new StringBuffer("aab,c,d");
        public String Separator = ",";
    }
    public static void main(String[] args) {
        StringAccum stringAccum = new StringAccum();
        if(stringAccum.total.toString().endsWith(",")){
            System.out.println(stringAccum.total.deleteCharAt(stringAccum.total.length()-1).toString());
        }else{
            System.out.println(stringAccum.total.toString());
        }

    }
}
