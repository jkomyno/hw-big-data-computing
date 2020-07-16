package hw2.convexhull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class use to compute the Convex Hull of a 2D point-set using Andrew's Monotone Chain algorithm and its diameter
 * using the Rotating Calipers method.
 */
public class MonotoneChain {
    /**
     * Stores the point-set
     */
    List<XYPoint> inputPoints;

    public MonotoneChain(List<XYPoint> inputPoints) {
        this.inputPoints = inputPoints;
    }

    /**
     * Scalar triple product, defined as the dot product of lst[-2] with the cross
     * product of lst[-1] and r.
     *
     * @param lst  list containing a series of points
     * @param rVec point
     * @return a boolean indicating if the signed area between three points is greater than zero
     */
    private static boolean isLeftTurn(Deque<XYPoint> lst, XYPoint rVec) {
        // qVec is lst[-1]
        XYPoint qVec = lst.getLast();
        lst.removeLast();

        // pVec is lst[-2]
        XYPoint pVec = lst.getLast();
        lst.addLast(qVec);

        return signedArea(pVec, qVec, rVec) > 0;
    }

    /**
     * Returns half of the area of the triangle formed by the given 3 points.
     *
     * @param a 1st point
     * @param b 2nd point
     * @param c 3rd point
     * @return the half the area of the triangle between the three points, might be negative
     */
    private static double signedArea(XYPoint a, XYPoint b, XYPoint c) {
        return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
    }

    /**
     * @param a 1st point
     * @param b 2nd point
     * @param c 3rd point
     * @return the absolute value of the signed area between the three points
     */
    private static double area(XYPoint a, XYPoint b, XYPoint c) {
        return Math.abs(signedArea(a, b, c));
    }

    /**
     * Returns the extreme points on the convex hull in normal format, i.e. in counterclockwise order starting from
     * the first point in the upper hull.
     * this.inputPoints must have at least 3 elements.
     *
     * @return the extreme points on the convex hull in counterclockwise order
     */
    public List<XYPoint> hull() {
        int size = this.inputPoints.size();

        if (size < 3) {
            return this.inputPoints;
        }

        // sort vectors in lexicographical orders. The first vector is the left-most point in the bottom.
        Collections.sort(this.inputPoints, (a, b) -> {
            int cmp = Double.compare(a.x, b.x);

            if (cmp == 0) {
                return Double.compare(a.y, b.y);
            } else {
                return cmp;
            }
        });

        // list containing the points in the upper part of the hull
        Deque<XYPoint> upper = new ArrayDeque<>();

        // list containing the points in the lower part of the hull
        Deque<XYPoint> lower = new ArrayDeque<>();

        // endpoints
        XYPoint p0 = this.inputPoints.get(0);
        XYPoint pN = this.inputPoints.get(size - 1);

        // initial lines of separation
        double ku = (pN.y - p0.y) / (pN.x - p0.x + 1e-12);
        double nu = p0.y - ku * p0.x;
        double kl = ku;
        double nl = nu;

        // add left endpoint
        upper.addFirst(p0);
        lower.addFirst(p0);

        // build middle of the upper/lower hull sections
        for (int j = 1; j < size - 1; j++) {
            XYPoint p = this.inputPoints.get(j);

            if (p.y > ku * p.x + nu) {
                while (upper.size() > 1 && isLeftTurn(upper, p)) {
                    upper.removeLast();
                }

                upper.addLast(p);

                // update upper line of separation
                ku = (pN.y - p.y) / (pN.x - p.x + 1e-12);
                nu = p.y - ku * p.x;

            } else if (p.y < kl * p.x + nl) {
                while (lower.size() > 1 && !isLeftTurn(lower, p)) {
                    lower.removeLast();
                }

                lower.addLast(p);

                // update upper line of separation
                kl = (pN.y - p.y) / (pN.x - p.x + 1e-12);
                nl = p.y - kl * p.x;
            }
        }

        while (upper.size() > 1 && isLeftTurn(upper, pN)) {
            upper.removeLast();
        }

        while (lower.size() > 1 && !isLeftTurn(lower, pN)) {
            lower.removeLast();
        }

        upper.addLast(pN);
        lower.removeFirst();

        // rotate the upper and lower lists to return a list of points in normal form

        List<XYPoint> upperList = upper.stream().collect(Collectors.toList());
        Collections.reverse(upperList);

        List<XYPoint> lowerList = lower.stream().collect(Collectors.toList());

        List<XYPoint> hullVertexesList = upperList;
        hullVertexesList.addAll(lowerList);

        // return reverse(upper) ++ lower
        return hullVertexesList;
    }

    /**
     * Rotating Calipers method for finding the maximum distance in a Convex Hull, i.e. its diameter.
     *
     * @return maximum pair-wise distance among the points of the Convex Hull, i.e. the diameter of the Hull
     */
    public double maxDistance() {
        // computes the Convex Hull
        List<XYPoint> hull = this.hull();

        int size = hull.size();

        // base case: only 1 point in the Hull, return 0
        if (size == 1) {
            return 0;
        }

        // base case: only 2 point in the Hull, return the distance between them
        if (size == 2) {
            double squareDistance = XYPoints.sqdist(hull.get(0), hull.get(1));
            return Math.sqrt(squareDistance);
        }

        // rotating calipers

        int k = 1;
        while (area(hull.get(size - 1), hull.get(0), hull.get((k + 1) % size)) >
                area(hull.get(size - 1), hull.get(0), hull.get(k))) {
            k++;
        }

        double maxDist = 0;
        for (int i = 0, j = k; i <= k && j < size; i++) {
            double currDist = XYPoints.sqdist(hull.get(i), hull.get(j));
            maxDist = Math.max(maxDist, currDist);

            while (j < size && area(hull.get(i), hull.get((i + 1) % size), hull.get((j + 1) % size)) >
                    area(hull.get(i), hull.get((i + 1) % size), hull.get(j))) {
                currDist = XYPoints.sqdist(hull.get(i), hull.get((j + 1) % size));
                maxDist = Math.max(maxDist, currDist);
                j++;
            }
        }

        // return the diameter of the convex-hull
        return Math.sqrt(maxDist);
    }
}
