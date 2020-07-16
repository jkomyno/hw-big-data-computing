package hw3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapReduce {
    /**
     * Implements the 4-approx MapReduce algorithm for diversity maximization of the set pointsRDD.
     * Extracts k points from each partition using Farthest-First Traversal in round1, in round2
     * collects all k*L extracted points in a coreset and runs runSequential over it to extract
     * the k most diverse points contained in the coreset.
     * @param pointsRDD set of points, RDD already repartitioned
     * @param k dimension of the returned point-set
     * @param L number of partitions
     * @return a list of vectors containing k points
     */
    public static List<Vector> runMapReduce(JavaRDD<Vector> pointsRDD, Integer k, Integer L) {
        /***********
         * ROUND 1 *
         ***********/

        // keep track of the time for round 1
        Stopwatch round1Stopwatch = Stopwatch.createStarted();

        // Since pointsRDD has already been partitioned in L different partitions during its initialization and we are
        // explicitly preserving the previous partitioning, the mapPartitions callback will be executed L times.
        final boolean preservesPartitioning = true;
        JavaRDD<Vector> round1 = pointsRDD.mapPartitions((it) -> {
            // S contains O(pointsRDD / L) points
            List<Vector> S = new ArrayList<>();
            it.forEachRemaining(point -> S.add(point));

            // collect the centers computed using Farthest-First Traversal on S
            List<Vector> centers = kCenterMPD(S, k);

            // return an iterator pointing to the sampled centers
            return centers.iterator();
        }, preservesPartitioning)
                // cache the round1 RDD to disk to ensure a proper time tracking
                .cache();

        // JavaRDD::count() is an eager operation, so it forces the evaluation of the previous block.
        // If we captured its result, its value would be (k * L)
        round1.count();

        // stop the timer and print the run-time of Round 1
        Duration round1Time = round1Stopwatch.stop();
        System.out.printf("Runtime of Round 1 = %d\n", round1Time.toMillis());

        /***********
         * ROUND 2 *
         ***********/

        // keep track of the time for round 2
        Stopwatch round2Stopwatch = Stopwatch.createStarted();

        // unites every set of centers in round1. coreset contains O(k * L) points.
        // Its concrete type is guaranteed to be an ArrayList<Vector>.
        // JavaRDD::collect() is an eager operation, so it forces the evaluation of the previous block.
        List<Vector> coreset = round1
                .collect();

        // pointSet contains k points which approximately maximize the diversity in coreset.
        // Its concrete type is guaranteed to be an ArrayList<Vector>
        List<Vector> pointSet = runSequential(coreset, k);

        // stop the timer and print the run-time of Round 2
        Duration round2Time = round2Stopwatch.stop();
        System.out.printf("Runtime of Round 2 = %d\n\n", round2Time.toMillis());

        return pointSet;
    }

    /**
     * Compute the exact Euclidean distance between all pairs of points in pointSet.
     * @param pointSet non-empty set of points
     * @return exact average pair-wise Euclidean distance of points in pointSet
     */
    public static double measure(List<Vector> pointSet) {
        int k = pointSet.size();

        // sum of distances
        double sum = 0;

        // number that keeps track of the computed distances
        int n = 0;

        // traverse all points without repetitions (i.e. i != j \forall i, j) to compute sum and n, which will be
        // used to compute the average pair-wise distance
        for (int i = 0; i < k - 1; i++) {
            for (int j = i + 1; j < k; j++) {
                // square distance between the i-th and j-th points
                double sqdist = Vectors.sqdist(pointSet.get(i), pointSet.get(j));

                // increase sum by the distance between the i-th and j-th points
                sum += Math.sqrt(sqdist);

                // increase number of computed distances
                n++;
            }
        }

        // return the exact average pair-wise Euclidean distance
        return sum / n;
    }

    /**
     * 2-approximated algorithm for diversity maximization. This implementation is provided by the Professors.
     * @param pointSet set of points
     * @param k number of points to return
     * @return k points that approximate the diversity maximization solution
     */
    public static List<Vector> runSequential(final List<Vector> pointSet, int k) {
        final int n = pointSet.size();
        if (k >= n) {
            return pointSet;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter = 0; iter < k / 2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i + 1; j < n; j++) {
                        if (candidates[j]) {
                            // Use squared euclidean distance to avoid an sqrt computation!
                            double d = Vectors.sqdist(pointSet.get(i), pointSet.get(j));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(pointSet.get(maxI));
            result.add(pointSet.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }

        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(pointSet.get(i));
                    break;
                }
            }
        }

        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
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
