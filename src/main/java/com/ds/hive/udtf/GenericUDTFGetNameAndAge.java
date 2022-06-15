package com.ds.hive.udtf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * TODO(分割一行数据)
 * @author   longju
 * @Date     2022年4月3日
原数据
id    name_age
1    赵文明:25;孙建国:36;王小花:19
2    李建军:40;赵佳佳:20

结果数据
id    name         age
1    赵文明        25
1    孙建国        36
1    王小花        19
2    李建军        40
2    赵佳佳        20

注册函数：nameagesplit
使用函数：select nameagesplit('赵文明:25;孙建国:36;王小花:19')
 */
public class GenericUDTFGetNameAndAge extends GenericUDTF {

    // 初始化方法
    // 该方法指定输入输出参数：输入的Object Inspectors和输出的Struct。
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        // 检查输入参数
        if (null != argOIs && argOIs.length == 1) {
            //判断是不是简单类型
            if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException("该函数只能接收简单类型的参数!");
            }

            //判断是不是String类型
            if (!argOIs[0].getTypeName().toUpperCase().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())) {
                throw new UDFArgumentException("该函数只能接收String类型的参数!");
            }

        }else {
            throw new UDFArgumentException("该函数需要接收参数，且只接收一个参数！");
        }
        // 设定返回值及输出的Struct
        List<String> structFieldNames = new ArrayList<String>();
        // 设定输出的参数名
        structFieldNames.add("name");
        structFieldNames.add("age");
        // 设定参数类型
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    private Object[] out = {new Text(),new IntWritable()};
    private String[] strs1 = null;
    private String[] strs2 = null;
    // 执行方法
    @Override
    public void process(Object[] args) throws HiveException {

        // 拆分数据 返回结构
        String param = String.valueOf(args[0]);
        // 拆分 赵文明:25;孙建国:36;王小花:19
        strs1 = param.split(";");
        // 循环
        for (String str : strs1) {
            // 赵文明:25
            strs2 = str.split(":");
            // 赵文明    25
            ((Text)out[0]).set(strs2[0]);
            ((IntWritable)out[1]).set(Integer.parseInt(strs2[1]));

            // 返回
            this.forward(out);
        }

    }
    // 资源释放方法
    @Override
    public void close() throws HiveException {

        // NOTHING

    }


}