package com.ds.flink.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;


/**
 * @author longju
 * @deprecated
 * */
public class JsonArrayParse  extends ScalarFunction {
    public String eval(String[] array){
        if (array.length == 0){
            return "";
        }else{
            String[] strArray = {"Convert","Array","With","Java"};
            StringBuilder stringBuilder =new StringBuilder();
            for(int i =0; i < array.length; i++) {
                stringBuilder.append(array[i]);
            }
            String joinedString = stringBuilder.toString();
            return joinedString;

        }

    }
}
