package hw1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class HW1 {
    public static void main(String[] args) {
        // two inputs needed: the number of Spark partitions and a file path to the dataset
        if (args.length != 2) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        /**
         * Spark initialization
         */

        // Spark configuration
        JavaSparkContext ctx = Config.createSparkConfig();

        /**
         * Input reading
         */

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read input file and subdivide it into K random partitions (exactly K)
        JavaRDD<String> docs = ctx.textFile(args[1]).repartition(K);

        /**
         * Deterministic class count
         */

        JavaPairRDD<String, Long> deterministicCount = MapReduce.deterministicClassCount(docs, K);
        System.out.println("VERSION WITH DETERMINISTIC PARTITIONS");
        // collecting the RDD count in this case is memory-safe because we are guaranteed the amount of distinct
        // classes in the dataset is in the order of 10.
        System.out.printf("Output pairs = %s%n%n", Utils.listToString(deterministicCount.collect()));

        /**
         * Partition class count
         */

        JavaPairRDD<String, Long> partitionCount = MapReduce.partitionClassCount(docs);

        // partition count in 2 JavaPairRDDs. The first one is the one with MAX_PARTITION_SIZE
        // We have taken the approach suggested in the email received by the Big Data Computing course assistants.
        Tuple2<JavaPairRDD<String, Long>, JavaPairRDD<String, Long>> countPartitioned =
                Utils.partition(partitionCount, pair -> pair._1() == Utils.MAX_PARTITIONS_SIZE);

        JavaPairRDD<String, Long> countWithPartitionSize = countPartitioned._1();
        JavaPairRDD<String, Long> countWithoutPartitionSize = countPartitioned._2();

        // nMax is the maximum number of pairs that in Round 1 are processed by a single reducer
        long nMax = countWithPartitionSize
                // we could have used the standard Comparator or another custom one to extract the maxPartitionsSize,
                // however since the results and time complexity would have been the same, we used the custom
                // comparator we already defined
                .max(Utils.classCountComparator)
                ._2();

        /**
         * classCountPairMax is the pair (className, count) where count is higher. In case there are multiple pairs
         * with the same highest count, the className which name is lexicographically smaller than the others is
         * returned.
         */
        Tuple2<String, Long> classCountPairMax = countWithoutPartitionSize
                // sum two numbers as a reduce operation
                .reduceByKey((x, y) -> x + y)
                .max(Utils.classCountComparator);

        System.out.println("VERSION WITH SPARK PARTITIONS");
        System.out.printf("Most frequent class = %s%n", classCountPairMax.toString());
        System.out.printf("Max partition size = %d%n", nMax);
    }
}
