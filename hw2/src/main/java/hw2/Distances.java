package hw2;

import hw2.convexhull.MonotoneChain;
import hw2.convexhull.XYPoint;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Distances {
    /**
     * Computes the maximum pairwise distance in the 2D point-set inputPoints using a brute-force approach.
     * Space complexity: O(1)
     * Time complexity: O(|S|^2)
     * @param S 2D point-set
     * @return Exact maximum pairwise distance in the point-set
     */
    public static double exactMPD(List<Vector> S) {
        // maximum square distance found up to now
        double maxSqDist = 0;
        int size = S.size();
        for (int i = 0; i < size - 1; i++) {
            for (int j = i + 1; j < size; j++) {
                double currSqDist = Vectors.sqdist(S.get(i), S.get(j));

                // update maxSqDist if the current square distance between points i and j is bigger than the previous one
                if (currSqDist > maxSqDist) {
                    maxSqDist = currSqDist;
                }
            }
        }

        // maxSqDist contains the maximum pair-wise square distance, so we need to take the square root of it
        return Math.sqrt(maxSqDist);
    }

    /**
     * Computes the maximum pairwise distance in the 2D point-set S using a computational geometry approach.
     * First, the Convex Hull of the point-set inputPoints is computed using Andrew's MonotoneChain algorithm.
     * Then, the diameter of the Convex Hull (i.e. the maximum pairwise distance) is computed using the Rotating
     * Calipers technique.
     * <p>
     * Space complexity: O(n)
     * Time complexity: O(n * log(n))
     *
     * @param S 2D point-set
     * @return Exact maximum pairwise distance in the point-set
     * <p>
     * INFO on Convex Hull: https://en.wikipedia.org/wiki/Convex_hull
     * INFO on MonotoneChain: https://en.wikibooks.org/wiki/Algorithm_Implementation/Geometry/Convex_hull/Monotone_chain
     * INFO on Rotating Calipers: https://en.wikipedia.org/wiki/Rotating_calipers
     */
    public static double exactConvexHullMPD(List<Vector> S) {
        // convert all the given input points into a list of XYpoints.
        List<XYPoint> inputXYPoints = S.stream()
                .map(vec -> {
                    double[] vecAsArray = vec.toArray();
                    double x = vecAsArray[0];
                    double y = vecAsArray[1];
                    return new XYPoint(x, y, vec);
                })
                .collect(Collectors.toList());
        MonotoneChain monotoneChain = new MonotoneChain(inputXYPoints);

        // compute and return the maximum distance i.e the diameter of the Convex Hull
        return monotoneChain.maxDistance();
    }

    /**
     * Computes a 2-approximation maximum distance in the n-dimensional point-set S.
     * Given an integer k < |S|, extract k random points in the point-set and compute the maximum distance
     * between all the k selected random points and all the remaining points (inputPoints - k selected points).
     * Space complexity: O(|S|)
     * Time complexity: O(k * |S|)
     *
     * @param S n-dimensional point-set
     * @param k number of k random points selected. 1 <= k <= |S| - 1
     * @return 2-approximation of the maximum pairwise distance in the point-set
     */
    public static double twoApproxMPD(List<Vector> S, int k) {
        // seeded random generator
        Random random = new Random(Utils.SEED);

        int size = S.size();
        int n = size;

        // we create a new copy because we are going to mutate the list, and S must remain pristine because it is used
        // in other algorithms as well
        List<Vector> inputPoints = new ArrayList<>(S);

        /**
         * S' is the set containing the k candidate points.
         * Since it is a HashSet, the {@link HashSet#contains} method has O(1) time complexity.
         */
        Set<Vector> SPrime = new HashSet<>();

        /**
         * Select k random points from inputPoints to SPrime without repetitions.
         * Since inputPoints is an instance of ArrayList, and {@link ArrayList#remove} has linear time complexity, we
         * use a smarter approach to remove items from it. For every random point we select, we decrease the upper-bound
         * of the random generator and move the item at the selected index to the end of the inputPoints list.
         * After we completed the selection, we delete the last k elements from inputPoints in O(1) time.
         */
        for (int i = 0; i < k;) {
            int index = random.nextInt(n);
            Vector point = inputPoints.get(index);

            if (!SPrime.contains(point)) {
                // move the item in inputPoints at index to the end of inputPoints in O(1)
                n--;
                Collections.swap(inputPoints, index, n);

                // add point to S' in O(1) amortized
                SPrime.add(point);
                i++;
            }
        }
        inputPoints = inputPoints.subList(0, n);
        // now SPrime has k elements, and inputPoints has size - k elements.

        // maximum square distance
        double maxSqDist = 0;

        // iterate through the k selected random point and compute all the distance with the remaining points
        for (Vector selected : SPrime) {
            for (Vector point : inputPoints) {
                double currSqDist = Vectors.sqdist(point, selected);

                // update maxDist if the current computed distance is bigger than the previous ones
                if (currSqDist > maxSqDist) {
                    maxSqDist = currSqDist;
                }
            }
        }

        // maxSqDist contains the maximum pair-wise square distance, so we need to return the square root of that value
        return Math.sqrt(maxSqDist);
    }

    /**
     * Discover and return a set C of k centers using the Farthest-First Traversal algorithm.
     * Space complexity: O(|S|)
     * Time complexity: O(k * |S|)
     *
     * @param S n-dimensional point-set
     * @param k number of selected centers. k < |S|
     * @return the k centers in S
     */
    public static List<Vector> kCenterMPD(List<Vector> S, Integer k) {
        // seeded random generator
        Random random = new Random(Utils.SEED);

        // arbitrary first center to be selected
        Vector c1 = S.get(random.nextInt(S.size()));

        // Set C of k centers to be computed. Initially it only has c1
        List<Vector> C = Stream.of(c1).collect(Collectors.toList());

        // we create a new copy because we are going to mutate the list, and S must remain pristine because it is used
        // in other algorithms as well. Since we want the removal of k elements from inputPoints to be as performant
        // as possible, we represent it as a Set.
        Set<Vector> inputPoints = S.stream().collect(Collectors.toSet());
        inputPoints.remove(c1);

        // Map that will contain the distance value of each point in S from the centers.
        Map<Vector, Double> maxMap = new HashMap<>();

        /**
         * Starting from the first random center, the distances between the points in S and the centers C are computed
         * and saved in maxMap.
         * The first point is chosen arbitrarily and each successive point is as far as possible from all previously
         * chosen points.
         * The outer while loop is computed k times. The inner for loop is computed O(|S|) times.
         * Selecting the maximum from maxMap is O(|S|).
         */

        // index of current center, from 0 to k-1.
        int i = 0;

        while (C.size() < k) {
            // iterate over inputPoints to find the new center candidate that maximizes the distance from C
            for (Vector point : inputPoints) {
                // current is the square distance between the i-th center in C and the current point
                double current = Vectors.sqdist(C.get(i), point);

                // insert the distance from the current point to the i-th center if it wasn't already computed
                if (!maxMap.containsKey(point)) {
                    maxMap.put(point, current);
                }

                // update the distance with the last center only if the distance is smaller than the previous value.
                //
                // NOTE: here we replace distance stored for the current not-selected point with the minimum of its
                // old value and the distance from point to the i-th center, as Farthest-First Traversal mandates.
                // However, we noticed that, for a sufficiently large k (e.g. k = 6 or k = 8), if we substituted the
                // smaller (<) comparator with the greater (>) comparator in the following if-statement, we would have
                // consistently better results for every tested dataset.
                //
                // That approach, however, isn't Farthest First Traversal anymore, so we limit ourself to report our
                // small discovery.
                else if (current < maxMap.get(point)) {
                    maxMap.put(point, current);
                }
            }

            // find the point that maximizes the distance from the centers.
            Vector indexMax = maxMap.entrySet()
                    .stream()
                    .max(Map.Entry.comparingByValue())
                    .get() // we invoke Optional::get directly because maxMap can't be empty
                    .getKey();

            // the new center is added to C and removed from inputPoints and maxMap.
            C.add(indexMax);
            inputPoints.remove(indexMax);
            maxMap.remove(indexMax);

            i++; // next center
        }

        // the centers are returned as a List.
        return C.stream().collect(Collectors.toList());
    }
}
