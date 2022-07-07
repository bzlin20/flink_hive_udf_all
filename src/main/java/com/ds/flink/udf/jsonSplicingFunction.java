package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Set;

/**
 * 将指定多个字符串用Json连接
 * 自定义标量函数需要继承{@link jsonSplicingFunction}，并为它实现eval方法
 *
 * @author: ds-longju
 * @date: 2022/6/27 10:11:47
 * @use  jsonSplicingFunction('deviceNo',deviceNo,'utcTime',utcTime,'longitude',longitude,'latitude',latitude)
 *
 * */
public class jsonSplicingFunction extends  ScalarFunction {
    public String eval(String... params) {
        String json = null;
        StringBuffer stringBuffer = new StringBuffer();
        if (params.length % 2 == 1) {
            return "该函数只能接收偶数个参数!";
        } else {
            // 参数插入到 hashmap里
            HashMap<String, String> paramMap = new HashMap<>();
            for (int i = 0; i < params.length - 1; i++) {
                paramMap.put(params[i], params[i + 1]);
                i = i + 1 ;
            }
            //json字符串的第一个位置应该是 {
            stringBuffer.append("{");
            //去除Map中的所有Key值，放入Set集合中
            Set<String> paramKey = paramMap.keySet();
            //遍历出每一个key值，然后取出Map中的对应value做非空判断，若非空就进行拼接到stringBuffer中
            for (String param : paramKey) {
                stringBuffer.append("\"" + param + "\":\"" + paramMap.get(param).toString().trim() + "\",");
            }
            // 去掉最后一个逗号
            json = stringBuffer.toString().substring(0,stringBuffer.length()-1) + "}";
            }
        return json;
    }

//    public static void main(String[] args)  {
//        jsonSplicingFunction jsonSplicingFunction = new jsonSplicingFunction();
//        String eval = jsonSplicingFunction.eval("aa", "1", "bb", "2","cc","3");
//        System.out.println(eval);
//    }
}
