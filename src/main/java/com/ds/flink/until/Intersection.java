package com.ds.flink.until;

import java.io.Serializable;

/**
 * @ClassName Intersection
 * @Description 几何体之间的关系类型
 * @Author longju
 **/
public enum Intersection implements Serializable {

    None,
    Tangent,
    Intersection,
    Containment;

    private static final long serialVersionUID = 4137231659891432206L;

}