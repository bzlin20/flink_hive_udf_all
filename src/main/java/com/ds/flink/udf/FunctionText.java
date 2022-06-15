package com.ds.flink.udf;
import com.ds.flink.udf.PointInPoly;

public class FunctionText {
    public static void main(String[] args) {
        PointInPoly pointInPoly = new PointInPoly();
        Long eval = pointInPoly.eval(122.00017495149577,29.770715645221202 ,"POLYGON((122.000581913185 29.7707133089897,122.000143154796 29.7702681167335,121.999655699193 29.770713428465,122.00007646126 29.7711487720778,122.000581913185 29.7707133089897))");
        System.out.println(eval);
        PointlnPolyString pointlnPolyString = new PointlnPolyString();
        Long eval1 = pointlnPolyString.eval("122.00017495149577", "29.770715645221202", "POLYGON((122.000581913185 29.7707133089897,122.000143154796 29.7702681167335,121.999655699193 29.770713428465,122.00007646126 29.7711487720778,122.000581913185 29.7707133089897))");
        System.out.println(eval1);
    }
}
