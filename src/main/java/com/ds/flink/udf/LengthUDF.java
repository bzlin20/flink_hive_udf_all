package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class LengthUDF extends ScalarFunction {
    public Integer eval(String[] array){
        return array.length;
    }
}
