package com.ds.hive.udtf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 数栈使用
 * select udtf_split(array("1","2","3","4"))
 */
public class UDTFJSONTuple extends GenericUDTF {
    private ListObjectInspector listOI = null;
    private final Object[] forwardObj = new Object[2];

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        if (args.length != 1) {
            throw new UDFArgumentException("posexplode() takes only one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("posexplode() takes an array as a parameter");
        }
        listOI = (ListObjectInspector) args[0];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("pos");
        fieldNames.add("val");
        fieldOIs.add(PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT));
        fieldOIs.add(listOI.getListElementObjectInspector());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] o) throws HiveException {
        List<?> list = listOI.getList(o[0]);
        if(list == null)
        {
            return;
        }

        for (int i = 0; i < list.size(); i++) {
            Object r = list.get(i);
            forwardObj[0] = new Integer(i);
            forwardObj[1] = r;
            forward(forwardObj);
        }
    }

    @Override
    public String toString() {
        return "posexplode";
    }
}
