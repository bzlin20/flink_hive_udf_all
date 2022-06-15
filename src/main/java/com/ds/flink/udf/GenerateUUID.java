package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.UUID;

/**
 * GenerateUUID
 *
 * @author: longju
 * @date: 2022/5/31 09:09:34
 */
public class GenerateUUID  extends ScalarFunction {
    public String eval(){
        return UUID.randomUUID().toString();
    }

    public boolean isDeterministic() {
        return false;
    }
}





