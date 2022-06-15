package com.ds.flink.udf;

import com.ds.flink.until.GeometryHelper;
import com.ds.flink.until.Intersection;
import com.ds.flink.until.Point2D;
import com.ds.flink.until.Polygon;
import java.util.ArrayList;

import org.apache.flink.table.functions.ScalarFunction;

public class PointInPoly  {
    public Long eval(Double lon, Double lat, String polygon) {
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
        Point2D point = new Point2D(lon, lat);
        GeometryHelper geometryHelper = new GeometryHelper();
        Intersection result = geometryHelper.intersectionOf(point, polyFinal);
        if (result == Intersection.Containment || result == Intersection.Tangent) {
            return 1L;
        } else {return 0L;}

    }

    public static void main(String[] args) {
        PointInPoly pointInPoly = new PointInPoly();
        Long eval = pointInPoly.eval(120.2126, 30.290851, "POLYGON(120.206324 30.293241,120.21289 30.286979,120.21864 30.294167))");
        System.out.println(eval);
    }
}
