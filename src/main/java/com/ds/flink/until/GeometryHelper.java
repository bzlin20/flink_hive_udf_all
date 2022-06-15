package com.ds.flink.until;

import com.ds.flink.until.Intersection;
import com.ds.flink.until.Line;
import com.ds.flink.until.Point2D;
import com.ds.flink.until.Polygon;

import java.io.Serializable;


public class GeometryHelper implements Serializable {

    private static final long serialVersionUID = 3220641372316598914L;

    /**
     * 判断线段与多边形的关系(改动较大)
     * @param line
     * @param polygon
     * @return
     */
    public static Intersection intersectionOf(Line line, Polygon polygon) {
        if (polygon.getPoints().size() / 2 == 0) {
            return Intersection.None;
        }
        if (polygon.getPoints().size() / 2 == 1) {
            Point2D point2D = new Point2D(polygon.getPoints().get(0), polygon.getPoints().get(1));
            return intersectionOf(point2D, line);
        }
        // 新增段：点在多边形内则相交
        Point2D beginPoint = new Point2D(line.getStartX(), line.getStartY());
        if (intersectionOf(beginPoint, polygon) == Intersection.Containment) {
            return Intersection.Intersection;
        }
        Point2D endPoint = new Point2D(line.getEndX(), line.getEndY());
        if (intersectionOf(endPoint, polygon) == Intersection.Containment) {
            return Intersection.Intersection;
        }
        // 新增段结束
        boolean tangent = false;
        Line line_origin = new Line(line.getStartX(), line.getStartY(), line.getEndX(), line.getEndY());
        for (int index = 0; index < polygon.getPoints().size() / 2; index++) {
            int index2 = (index + 1) % (polygon.getPoints().size() / 2);
            Line line1 = new Line(line_origin.getStartX(), line_origin.getStartY(), line_origin.getEndX(), line_origin.getEndY());
            Line line2 = new Line(polygon.getPoints().get(index * 2),
                    polygon.getPoints().get(index * 2 + 1),
                    polygon.getPoints().get(index2 * 2),
                    polygon.getPoints().get(index2 * 2 + 1));
            Intersection intersection = intersectionOf(line1, line2);
            if (intersection == Intersection.Intersection) {
                return intersection;
            }
            if (intersection == Intersection.Tangent) {
                tangent = true;
            }
        }

        Point2D p1 = new Point2D(line.getStartX(), line.getStartY());
        return tangent ? Intersection.Tangent : intersectionOf(p1, polygon);
    }

    /**
     * 判断点与多边形的关系
     * @param point
     * @param polygon
     * @return
     */
    public static Intersection intersectionOf(Point2D point, Polygon polygon) {

        switch (polygon.getPoints().size() / 2) {
            case 0:
                return Intersection.None;
            case 1:
                Point2D point2D = new Point2D(polygon.getPoints().get(0), polygon.getPoints().get(1));
                if (point2D.getX() == point.getX() && point2D.getY() == point.getY()) {
                    return Intersection.Tangent;
                } else {
                    return Intersection.None;
                }
            case 2:
                return intersectionOf(point, new Line(polygon.getPoints().get(0),
                        polygon.getPoints().get(1),
                        polygon.getPoints().get(2),
                        polygon.getPoints().get(3)));
        }

        int counter = 0;
        int i;
        Point2D p1 = new Point2D(polygon.getPoints().get(0), polygon.getPoints().get(1));
        int n = polygon.getPoints().size() / 2;
        if (point.getX() == p1.getX() && point.getY() == p1.getY()) {
            return Intersection.Tangent;
            
        }
        for (i = 1; i <= n; i++) {
            Point2D p2 = new Point2D(polygon.getPoints().get((i % n) * 2), polygon.getPoints().get((i % n) * 2 + 1));
            if (point.getX() == p2.getX() && point.getY() == p2.getY()) {
                return Intersection.Tangent;
            }
            if (intersectionOf(point, new Line(p1.getX(),p1.getY(),p2.getX(),p2.getY())) == Intersection.Tangent) {
                return Intersection.Tangent;
            }
            if (point.getY() > Math.min(p1.getY(), p2.getY())) {
                if (point.getY() <= Math.max(p1.getY(), p2.getY())) {
                    if (point.getX() <= Math.max(p1.getX(), p2.getX())) {
                        if (p1.getY() != p2.getY()) {
                            double xinters = (point.getY() - p1.getY()) * (p2.getX() - p1.getX()) / (p2.getY() - p1.getY()) + p1.getX();
                            if (p1.getX() == p2.getX() || point.getX() <= xinters)
                                counter++;
                        }
                    }
                }
            }
            p1 = p2;
        }

        return (counter % 2 == 1) ? Intersection.Containment : Intersection.None;
    }

    /**
     * 判断点与直线的关系
     * @param point
     * @param line
     * @return
     */
    public static Intersection intersectionOf(Point2D point, Line line) {
        float bottomY = (float) Math.min(line.getStartY(), line.getEndY());
        float topY = (float) Math.max(line.getStartY(), line.getEndY());
        boolean heightIsRight = point.getY() >= bottomY &&
                point.getY() <= topY;
        //Vertical line, slope is divideByZero error!
        if (line.getStartX() == line.getEndX()) {
            if (point.getX() == line.getStartX() && heightIsRight) {
                return Intersection.Tangent;
            } else {
                return Intersection.None;
            }
        }
        float slope = (float) ((line.getEndY() - line.getStartY()) / (line.getEndX() - line.getStartX()));
        boolean onLine = (line.getStartY() - point.getY()) == (slope * (line.getStartX() - point.getX()));
        if (onLine && heightIsRight) {
            return Intersection.Tangent;
        } else {
            return Intersection.None;
        }
    }

    /**
     * 判断直线与直线的关系
     * @param line1
     * @param line2
     * @return
     */
    public static Intersection intersectionOf(Line line1, Line line2) {
        //  Fail if either line segment is zero-length.
        if (line1.getStartX() == line1.getEndX() && line1.getStartY() == line1.getEndY() || line2.getStartX() == line2.getEndX() && line2.getStartY() == line2.getEndY())
            return Intersection.None;

        if (line1.getStartX() == line2.getStartX() && line1.getStartY() == line2.getStartY() || line1.getEndX() == line2.getStartX() && line1.getEndY() == line2.getStartY())
            return Intersection.Intersection;
        if (line1.getStartX() == line2.getEndX() && line1.getStartY() == line2.getEndY() || line1.getEndX() == line2.getEndX() && line1.getEndY() == line2.getEndY())
            return Intersection.Intersection;

        //  (1) Translate the system so that point A is on the origin.
        line1.setEndX(line1.getEndX() - line1.getStartX());
        line1.setEndY(line1.getEndY() - line1.getStartY());
        line2.setStartX(line2.getStartX() - line1.getStartX());
        line2.setStartY(line2.getStartY() - line1.getStartY());
        line2.setEndX(line2.getEndX() - line1.getStartX());
        line2.setEndY(line2.getEndY() - line1.getStartY());

        //  Discover the length of segment A-B.
        double distAB = Math.sqrt(line1.getEndX() * line1.getEndX() + line1.getEndY() * line1.getEndY());

        //  (2) Rotate the system so that point B is on the positive X axis.
        double theCos = line1.getEndX() / distAB;
        double theSin = line1.getEndY() / distAB;
        double newX = line2.getStartX() * theCos + line2.getStartY() * theSin;
        line2.setStartY((float) (line2.getStartY() * theCos - line2.getStartX() * theSin));
        line2.setStartX((float) newX);
        newX = line2.getEndX() * theCos + line2.getEndY() * theSin;
        line2.setEndY((float) (line2.getEndY() * theCos - line2.getEndX() * theSin));
        line2.setEndX((float) newX);

        //  Fail if segment C-D doesn't cross line A-B.
        if (line2.getStartY() < 0 && line2.getEndY() < 0 || line2.getStartY() >= 0 && line2.getEndY() >= 0)
            return Intersection.None;

        //  (3) Discover the position of the intersection point along line A-B.
        double posAB = line2.getEndX() + (line2.getStartX() - line2.getEndX()) * line2.getEndY() / (line2.getEndY() - line2.getStartY());

        //  Fail if segment C-D crosses line A-B outside of segment A-B.
        if (posAB < 0 || posAB > distAB)
            return Intersection.None;

        //  (4) Apply the discovered position to line A-B in the original coordinate system.
        return Intersection.Intersection;
    }
}
