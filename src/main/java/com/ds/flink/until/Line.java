package com.ds.flink.until;

import java.io.Serializable;

/**
 * @ClassName Line
 * @Description 线
 * @Author weiwei
 **/
public class Line implements Serializable {

    private static final long serialVersionUID = 2316598914322064137L;

    /**
     * 起点x
     */
    private double startX;
    /**
     * 起点y
     */
    private double startY;
    /**
     * 终点x
     */
    private double endX;
    /**
     * 终点y
     */
    private double endY;

    public double getStartX() {
        return startX;
    }

    public double getStartY() {
        return startY;
    }

    public double getEndX() {
        return endX;
    }

    public double getEndY() {
        return endY;
    }

    public void setStartX(double startX) {
        this.startX = startX;
    }

    public void setStartY(double startY) {
        this.startY = startY;
    }

    public void setEndX(double endX) {
        this.endX = endX;
    }

    public void setEndY(double endY) {
        this.endY = endY;
    }

    /**
     * Creates a new instance of Line.
     * @param startX the horizontal coordinate of the start point of the line segment
     * @param startY the vertical coordinate of the start point of the line segment
     * @param endX the horizontal coordinate of the end point of the line segment
     * @param endY the vertical coordinate of the end point of the line segment
     */
    public Line(double startX, double startY, double endX, double endY) {
        this.startX = startX;
        this.startY = startY;
        this.endX = endX;
        this.endY = endY;
    }

    public Line() {

    }

}

