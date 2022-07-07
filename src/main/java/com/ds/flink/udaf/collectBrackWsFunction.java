package com.ds.flink.udaf;

import org.apache.flink.table.functions.AggregateFunction;


/**
 * @author ds-longju
 * @function 实现hive collect_ws 函数功能，目前是用","分隔，可以自己自定义,拼接后成数组
 *
 * */
public class collectBrackWsFunction  extends AggregateFunction<String,collectBrackWsFunction.StringAccum> {


    /**
     *  定义初始终变量
     * 定义存放count UDAF状态的accumulator的数据的结构。
     */
    public static class StringAccum {
        public StringBuffer total;
    }

    /**
     * 中间结果
     * getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法。
     * @param stringAccum
     */
    @Override
    public String  getValue(collectBrackWsFunction.StringAccum stringAccum) {
        if (stringAccum.total.length() == 0){
            return "" ;
        }
        String baseStr = stringAccum.total.toString();
        String sinkStr="[" + baseStr.substring(0, baseStr.length() - 1) +  "]";
        return sinkStr;

    }

    /**
     * 初始化count UDAF的accumulator。
     */
    @Override
    public collectBrackWsFunction.StringAccum createAccumulator() {
        collectBrackWsFunction.StringAccum acc = new collectBrackWsFunction.StringAccum();
        acc.total =new StringBuffer("") ;
        return acc;
    }
    /**
     * accumulate提供了如何根据输入的数据更新count。
     *
     * @param accumulator 累加器
     * @param iValue      函数输入
     * Separator  连接分割符
     */
    public void accumulate(collectBrackWsFunction.StringAccum accumulator, String iValue) {
        StringBuffer sbiValue = new StringBuffer(iValue + ",");
        accumulator.total.append(sbiValue);
    }

    /**
     *
     * 使用merge方法把多个accumulator合为1个accumulator
     * merge方法的第1个参数，必须是使用AggregateFunction的ACC类型的accumulator，而且第1个accumulator是merge方法完成之后，状态所存放的地方。
     * merge方法的第2个参数是1个ACC type的accumulator遍历迭代器，里面有可能存在1个或者多个accumulator。
     * 例如session window。由于实时计算Flink版具有out of order的特性，后输入的数据有可能位于2个原本分开的session中间，
     * 这样就把2个session合为1个session。此时，需要使用merge方法把多个accumulator合为1个accumulator
     */

    public void merge(collectBrackWsFunction.StringAccum accumulator, Iterable<collectBrackWsFunction.StringAccum>its) {
        for (collectBrackWsFunction.StringAccum other : its) {
            accumulator.total.append(other.total);
        }

    }




}
