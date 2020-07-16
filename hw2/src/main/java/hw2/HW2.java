package hw2;

import hw2.convexhull.MonotoneChain;
import hw2.outputstrategy.ExactOutputStrategy;
import hw2.outputstrategy.KCenterOutputStrategy;
import hw2.outputstrategy.OutputStrategy;
import hw2.outputstrategy.TwoApproxOutputStrategy;
import org.apache.spark.mllib.linalg.Vector;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * @NOTE: we made 2 different implementations of the exact algorithm.
 * - The first one uses the brute force approach as suggested in the homework, with a time complexity quadratic in the
 * size of the input. It works with vectors of any dimensions but it's extremely slow, as it takes hours to compute
 * the exact max-distance of the uber-medium.csv and uber-large.csv datasets.
 * - The second one uses a computational geometric approach. In particular, it first computes the Convex Hull of the
 * given points set, and then determines its diameter, i.e. the max-distance between the points in the Hull.
 * It uses Andrew's Monotone Chain algorithm to compute the Hull, and the Rotating Calipers method to compute
 * the diameter of said Hull.
 * Its time complexity is linearithmic on the size of the input. The drawbacks in this case are that it only supports
 * 2D points (although we suspect it may be extended to support 3D points as well) and its implementation is less
 * straightforward to read. See the {@link MonotoneChain} class.
 */
public class HW2 {
    /**
     * Boolean flag that enables/disables the exact brute-force algorithm for computing the maximum pair-wise
     * distance. This is useful when medium or large datasets are fed in as input, to reduce computation time from
     * hours to seconds.
     */
    private final static boolean IS_EXACT_BRUTE_FORCE_ALGORITHM_ENABLED = false;

    /**
     * Boolean flag that enables/disables the exact algorithm based on Convex Hull for computing the maximum
     * pair-wise distance.
     */
    private final static boolean IS_CONVEX_HULL_ALGORITHM_IF_2D_ENABLED = true;

    /**
     * @throws IllegalArgumentException if the user doesn't provide exactly the 2 required arguments, or if the given
     *                                  file path is a directory and not a file
     * @throws IOException              if the file path isn't readable
     * @throws NumberFormatException    if k is not a number, < 1, or greater than the number of points in input
     */
    public static void main(String[] args) throws Exception {
        // two inputs needed, a file path and an integer k
        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: file_path k");
        }

        /**
         * Input reading
         */

        String filename = args[0];
        String kStr = args[1];

        // as of Java8, the returned List is guaranteed to have concrete type ArrayList
        List<Vector> inputPoints = Utils.readVectorsSeq(filename);

        // 1 <= k <= inputPoints.size() - 1
        Integer k = Utils.parseK(kStr, inputPoints.size());

        // get the dimension of the input space, under the assumption that any given point has the same dimension
        int dimensions = inputPoints.get(0).size();

        /**
         * Execute the max-distance algorithms.
         * Their execution time is measured using the {@link Stopwatch} class.
         * We abstracted out the process of printing the results of the various algorithm using the
         * {@link OutputStrategy} class, which implements the Strategy pattern.
         * It's thus easy to extend this homework to multiple other algorithms using a single common method to print
         * the results (@see the {@link G07HW2#printResults} method).
         */

        if (IS_EXACT_BRUTE_FORCE_ALGORITHM_ENABLED) {
            // calculate the max-distance exactly, using exactMPD, and measure the time required for its completion.
            Stopwatch exactMPDStopwatch = Stopwatch.createStarted();
            double exactMPDMaxDistance = Distances.exactMPD(inputPoints);
            Duration exactMPDTime = exactMPDStopwatch.stop();

            printResults(new ExactOutputStrategy(exactMPDMaxDistance, exactMPDTime));
        }

        if (IS_CONVEX_HULL_ALGORITHM_IF_2D_ENABLED && dimensions == 2) {
            // calculate the max-distance exactly, using the Convex Hull method, and measure the time required for its
            // completion. This method only works in the Euclidean 2D space
            Stopwatch exactConvexHullMPDStopwatch = Stopwatch.createStarted();
            double exactConvexHullMPDMaxDistance = Distances.exactConvexHullMPD(inputPoints);
            Duration exactConvexHullMPDTime = exactConvexHullMPDStopwatch.stop();

            printResults(new ExactOutputStrategy(exactConvexHullMPDMaxDistance, exactConvexHullMPDTime));
        }

        // calculate the max-distance using the 2-approx algorithm, twoApproxMPD, and measure the time required for its
        // completion.
        Stopwatch twoApproxMPDStopwatch = Stopwatch.createStarted();
        double twoApproxMPDMaxDistance = Distances.twoApproxMPD(inputPoints, k);
        Duration twoApproxMPDTime = twoApproxMPDStopwatch.stop();

        printResults(new TwoApproxOutputStrategy(twoApproxMPDMaxDistance, twoApproxMPDTime), k);

        // calculate the max-distance using the k-center method, kCenterMPD, followed by the use of exactMPD on the
        // centers, and measure the time required for its completion.
        Stopwatch kCenterMPDStopwatch = Stopwatch.createStarted();
        List<Vector> centers = Distances.kCenterMPD(inputPoints, k);
        double kCenterMPDMaxDistance = Distances.exactMPD(centers);
        Duration kCenterMPDTime = kCenterMPDStopwatch.stop();
        printResults(new KCenterOutputStrategy(kCenterMPDMaxDistance, kCenterMPDTime), k);
    }

    /**
     * Prints the results of the algorithms in the format required by the homework specification.
     *
     * @param outputStrategy concrete implementation of OutputStrategy
     * @param k              number of k centers
     */
    private static void printResults(OutputStrategy outputStrategy, Integer k) {
        System.out.printf("%s ALGORITHM\n", outputStrategy.title);
        if (k > 0) {
            System.out.printf("k = %d\n", k);
        }
        System.out.printf("Max distance = %.15f\n", outputStrategy.maxDistance);
        System.out.printf("Running time = %d\n\n", outputStrategy.runningTime.toMillis());
    }

    /**
     * Overload of printResults that sets k to 0 in the case where a ExactOutputStrategy instance is given in input.
     *
     * @param outputStrategy ExactOutputStrategy instance
     */
    private static void printResults(ExactOutputStrategy outputStrategy) {
        printResults(outputStrategy, 0);
    }
}
