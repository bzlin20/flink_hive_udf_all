package com.ds.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author ds-longju
 * @date 20220621
 */
public class regexpExtract  extends ScalarFunction {
    /**
     * @param content：待提取的字符串
     * @param regEx: 正则表达式
     * @param number :取数位置
     *
     * */
    public String eval(String content, String regEx, Integer number) {
        if(content.length()==0){
            return "";
        }else {
            Pattern pattern = Pattern.compile(regEx);
            Matcher matcher = pattern.matcher(content);
            if (matcher.find( )) {
                return matcher.group(number);
            } else {
                return "";
            }
        }
    }

//    public static void main(String[] args) {
//        regexpExtract regexpExtract = new regexpExtract();
//        String eval = regexpExtract.eval("RT34", "^([R][T])([0-9]{2,3})$", 2);
//        System.out.println(eval);
//    }
}
