package com.ds.flink.until;


import java.io.Serializable;

/**
 * @ClassName Point2D
 * @Description 点类
 * @Author longju
 **/
public class Point2D implements Serializable {

    private static final long serialVersionUID = 6413799165989143220L;

    /**
     * Point or vector with both coordinates set to 0.
     */
    public Point2D(double x, double y){
        this.x = x;
        this.y = y;
    }
    /**
     * The x coordinate.
     *
     * @defaultValue 0.0
     */
    private double x;


    /**
     * The y coordinate.
     *
     * @defaultValue 0.0
     */
    private double y;

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}


