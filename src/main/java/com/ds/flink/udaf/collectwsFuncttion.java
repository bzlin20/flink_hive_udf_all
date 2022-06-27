package com.ds.flink.udaf;

import org.apache.flink.table.functions.AggregateFunction;


public class collectwsFuncttion extends AggregateFunction<String,collectwsFuncttion.CountAccum> {

    /**
     * 定义存放count UDAF状态的accumulator的数据的结构。
     */
    public static class CountAccum {
        public StringBuffer total;
    }


    /**
     * getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法。
     * @param countAccum
     */
    @Override
    public String  getValue(collectwsFuncttion.CountAccum countAccum) {
        return countAccum.total.toString();
    }

    /**
     * 初始化count UDAF的accumulator。
     */
    @Override
    public collectwsFuncttion.CountAccum createAccumulator() {
        collectwsFuncttion.CountAccum acc = new collectwsFuncttion.CountAccum();
        acc.total =new StringBuffer("") ;
        return acc;
    }

    /**
     * accumulate提供了如何根据输入的数据更新count。
     *
     * @param accumulator 累加器
     * @param iValue      函数输入
     */
    public void accumulate(collectwsFuncttion.CountAccum accumulator, String iValue) {
        StringBuffer sbiValue = new StringBuffer(iValue);
        accumulator.total.append(sbiValue);
    }

    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total.append(other.total);
        }
    }
}
