package com.ds.flink.until;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

/**
 * @ClassName Polygon
 * @Description 多边形
 * @Author longju
 **/
public class Polygon implements Serializable {

    private static final long serialVersionUID = 1432206413799165989L;

    /**
     * 点的列表
     */
    private List<Double> points;

    public List<Double> getPoints() {
        return points;
    }

    /**
     * 无参构造函数
     */
    public Polygon(){
        this.points = new ArrayList();
    }

}
