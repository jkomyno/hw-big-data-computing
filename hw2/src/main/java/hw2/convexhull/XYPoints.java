package hw2.convexhull;

import org.apache.spark.mllib.linalg.Vectors;

/**
 * Wrapper for Vectors.sqdir compatible with points represented as XYPoint
 */
public class XYPoints {
    public static double sqdist(XYPoint a, XYPoint b) {
        return Vectors.sqdist(a.vector, b.vector);
    }
}