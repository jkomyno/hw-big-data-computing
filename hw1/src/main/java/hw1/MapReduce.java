package hw1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapReduce {
    public static JavaPairRDD<String, Long> deterministicClassCount(JavaRDD<String> docs, Integer K) {
        return docs
                .mapToPair((document) -> {
                    // separate items on a single line of the dataset
                    String[] row = document.split(" ");

                    // index of the row as a Long in base 10, e.g. 0
                    final Long index = Long.valueOf(row[0], 10);

                    // className of the row, e.g. "Action"
                    final String className = row[1];

                    // map each pair into an intermediate pair (index mod K, className)
                    return new Tuple2<>(index % K, className);
                })
                .groupByKey()
                .flatMapToPair((pair) -> {
                    // The reduce phase of MapReduce can be implemented by applying flatMapToPair after groupByKey

                    // Map to keep track of the partial number of occurrences of a particular className.
                    // The className is the key in the map, whereas the value is its partial count.
                    Map<String, Long> countMap = new HashMap<>();

                    // count each partial occurrence and store them into countMap as values
                    for (String className : pair._2()) {
                        countMap.merge(className, 1L, (x, y) -> x + y);
                    }

                    // transform the count map in a list of pairs [(className, count)]
                    List<Tuple2<String, Long>> pairs = Utils.mapToListOfPairs(countMap);
                    return pairs.iterator();
                })
                // sum two numbers as a reduce operation
                .reduceByKey((x, y) -> x + y)
                .sortByKey(); // sort classes by lexicographical order
    }

    public static JavaPairRDD<String, Long> partitionClassCount(JavaRDD<String> docs) {
        return docs
                .mapToPair((document) -> {
                    // separate items on a single line of the dataset
                    String[] row = document.split(" ");

                    // index of the row as a Long in base 10, e.g. 0
                    final Long index = Long.valueOf(row[0], 10);

                    // className of the row, e.g. "Action"
                    final String className = row[1];

                    /**
                     * map each pair into an intermediate pair (index, className).
                     * We let Spark decide the partitioning (thanks to the previous .repartition(K) call).
                     */
                    return new Tuple2<>(index, className);
                })
                .mapPartitionsToPair((it) -> {
                    /**
                     * Map to keep track of the partial number of occurrences of a particular className.
                     * The className is the key in the map, whereas the value is its partial count.
                     */
                    Map<String, Long> countMap = new HashMap<>();

                    /**
                     * count each partial occurrence and store them into countMap as values.
                     * n keeps track of the number of pairs received from the iterator it.
                     */
                    long n = 0L;
                    while (it.hasNext()) {
                        Tuple2<Long, String> tuple = it.next();
                        countMap.merge(tuple._2(), 1L, (x, y) -> x + y);
                        n++;
                    }

                    /**
                     * Add the special entry ("maxPartitionSize", n) to countMap, where n is the number of pairs
                     * that in Round 1 are processed by this single reducer. Each partition is processed
                     * by a single reducer.
                     */
                    countMap.put(Utils.MAX_PARTITIONS_SIZE, n);

                    // transform the count map in a list of pairs [(className, count)]
                    List<Tuple2<String, Long>> pairs = Utils.mapToListOfPairs(countMap);
                    return pairs.iterator();
                })
                /**
                 * Optimize future calls to .filter(). The next operation on this RDD will branch out its lineage,
                 * thus we want to avoid expensive reloading from the storage.
                 * For reference: https://stackoverflow.com/a/28984561/6174476
                 */
                .cache();
    }
}
