package hw3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class HW3 {
    /**
     * @throws IllegalArgumentException if the user doesn't provide exactly the 3 required arguments, or if the given
     *                                  file path is a directory and not a file
     * @throws IOException              if the file path isn't readable
     * @throws NumberFormatException    if k is not a number, < 1, or greater than the number of points in input
     */
    public static void main(String[] args) throws Exception {
        // initialitazion started
        Stopwatch initStopwatch = Stopwatch.createStarted();

        // three inputs needed, a file path and two integers k and L
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: file_path k L");
        }

        /**
         * Spark initialization
         */

        JavaSparkContext ctx = Config.createSparkConfig();

        /**
         * Input reading
         */

        String filename = args[0];
        String kStr = args[1];
        String LStr = args[2];

        Integer L = Utils.parseL(LStr);

        // Read input file and subdivide it into exactly L partitions
        JavaRDD<Vector> inputPoints = ctx.textFile(filename)
                .map(Utils::strToVector)
                .repartition(L)
                .cache();

        long size = inputPoints.count();

        // 1 <= k <= size - 1
        Integer k = Utils.parseK(kStr, size);

        // initialization ended
        Duration initTime = initStopwatch.stop();

        // print info and time taken
        System.out.printf("Number of points = %d\n", size);
        System.out.printf("k = %d\n", k);
        System.out.printf("L = %d\n", L);
        System.out.printf("Initialization time = %d\n\n", initTime.toMillis());

        // run the 4-approx MapReduce algorithm for diversity maximization on inputPoints.
        // solutions's concrete type is guaranteed to be an ArrayList<Vector>
        List<Vector> solution = MapReduce.runMapReduce(inputPoints, k, L);

        // compute the averageDistance of the solution and print it
        double averageDistance = MapReduce.measure(solution);
        System.out.printf("Average distance = %.15f\n",averageDistance);
    }
}
