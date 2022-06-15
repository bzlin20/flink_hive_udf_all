package com.ds.flink.udf;

import com.ds.flink.until.GeometryHelper;
import com.ds.flink.until.Intersection;
import com.ds.flink.until.Point2D;
import com.ds.flink.until.Polygon;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;

public class PointlnPolyString  extends ScalarFunction {
    public Long eval(String lon, String lat, String polygon) {
        // 处理传入的polygon字符串
        String poly = polygon.replace("POLYGON", "").replace("(", "").replace(")", "");
        String[] points =  poly.split(",");
        ArrayList polyReady = new ArrayList();
        for (int i = 0; i < points.length; i++) {
            String[] aPoint = points[i].split(" ");
            polyReady.add(Double.valueOf(aPoint[0]));
            polyReady.add(Double.valueOf(aPoint[1]));
        }
        Polygon polyFinal = new Polygon();
        polyFinal.getPoints().addAll(polyReady);
        // 判断点是否在多边形内
        Point2D point = new Point2D(Double.valueOf(lon), Double.valueOf(lat));
        GeometryHelper geometryHelper = new GeometryHelper();
        Intersection result = geometryHelper.intersectionOf(point, polyFinal);
        if (result == Intersection.Containment || result == Intersection.Tangent) {
            return 1L;
        } else {return 0L;}

    }
}
