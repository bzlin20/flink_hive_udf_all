package com.ds.flink.udaf;


import org.apache.flink.table.functions.AggregateFunction;

/**
 * 求和函数
 * <p>
 * 自定义表聚合函数需要继承{@link AggregateFunction}，并实现必要的方法：
 * <ul>
 *   <li>{@code createAccumulator}
 *   <li>{@code accumulate}
 *   <li>{@code getValue}
 * </ul>
 *
 * @author: longju
 * @date: 2022/05/31 11:06:08
 */
public class CountFunction extends AggregateFunction<Long, CountFunction.CountAccum> {

    /**
     * 定义存放count UDAF状态的accumulator的数据的结构。
     */
    public static class CountAccum {
        public long total;
    }

    @Override
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.total = 0;
        return acc;
    }

    /**
     * accumulate提供了如何根据输入的数据更新count。
     *
     * @param accumulator 累加器
     * @param iValue      函数输入
     */
    public void accumulate(CountAccum accumulator, Integer iValue) {
        accumulator.total += iValue;
    }


    @Override
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }


    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total += other.total;
        }
    }
}
