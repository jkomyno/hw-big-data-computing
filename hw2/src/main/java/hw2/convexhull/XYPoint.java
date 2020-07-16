package hw2.convexhull;

import org.apache.spark.mllib.linalg.Vector;

/**
 * 2D point representation. This commodity class serves 2 purposes:
 * - provide a clearer MonotoneChain implementation, without cluttering the code with many vector.toArray() calls
 * - make it explicit to the programmer that we're dealing with 2D points, and not vectors in generic dimensions.
 */
public class XYPoint {
    public final double x;
    public final double y;

    /**
     * Original vector representation, needed for XYPoints.sqdist()
     */
    public final Vector vector;

    public XYPoint(double x, double y, Vector vector) {
        this.x = x;
        this.y = y;
        this.vector = vector;
    }
}
