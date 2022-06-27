package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class timeApply extends  ScalarFunction {
    /**
     * @param timeStr : 字符串格式 2022-06-22T14:09:33.6052451+08:00
     * @param startTime : 当前时间早多少秒
     * @param endTime :当前时间晚多少秒
     * */
    public Long eval(String timeStr,Integer startTime,Integer endTime){
        long date = System.currentTimeMillis();
        long start_time = date - 1000 * startTime;
        long end_time = date + 1000 * endTime ;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            long eventTime = formatter.parse(timeStr).getTime();
            if(start_time <= eventTime && eventTime <= end_time) {
                return 1L;
            }else{
                return 0L;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0L;
    }

}
