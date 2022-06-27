package com.ds.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 *
 *	将大数归一化为-10到10之间的数字
 * */
public class Normalize  extends UDF {
    public double evaluate(double input) {
        int len;
        Double res = new Double(input);
        if (res.isNaN())
            return 0.0D;
        Integer int_input = Integer.valueOf(res.intValue());
        if (input < 0.0D) {
            len = int_input.toString().length() - 1;
        } else {
            len = int_input.toString().length();
        }
        res = Double.valueOf(input / Math.pow(10.0D, (len - 1)));
        return res.doubleValue();
    }

}
