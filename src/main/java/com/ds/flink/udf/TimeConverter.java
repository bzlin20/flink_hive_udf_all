package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 将指定格式的时间字符串转换为时间戳long类型
 * 自定义标量函数需要继承{@link ScalarFunction}，并为它实现eval方法
 *
 * @author: ds-longju
 * @date: 2022/6/27 10:11:47
 *
 * */
public class TimeConverter extends ScalarFunction {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 方法必须公开声明为eval，可重载。详情见 {@link ScalarFunction}
     *
     * @param time 时间字符串，例如：2022-04-01 11:11:11
     * @return 时间戳
     */
    public long eval(String time) {
        LocalDateTime ldt = LocalDateTime.parse(time, formatter);

        return ldt.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}