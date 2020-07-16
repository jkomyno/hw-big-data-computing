package hw1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
    public final static String MAX_PARTITIONS_SIZE = "maxPartitionsSize";

    /**
     * Convert a List to a String. It doesn't limit itself to calling List.toString().
     * Instead, it removes the square brackets and commas from the String representation of a list,
     * according to the homework specification.
     */
    public static <T> String listToString(List<T> list) {
        StringBuilder sb = new StringBuilder();

        for (T s : list) {
            sb.append(s.toString());
            sb.append(" ");
        }

        return sb.toString();
    }

    /**
     * Transforms a map into a list of Tuple2.
     */
    public static <K, V> List<Tuple2<K, V>> mapToListOfPairs(Map<K, V> map) {
        return map.entrySet()
                .stream()
                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Creates a binary partition of a JavaPairRDD according to the given predicate.
     * The elements of the RDD that satisfy the predicate are returned as first element of the tuple, the ones
     * that don't are returned as second element of the tuple.
     * @param pairRDD JavaPairRDD to partition in two according to the given predicate
     * @param predicate Unary function returning a boolean used to filter
     */
    public static <K, V> Tuple2<JavaPairRDD<K, V>, JavaPairRDD<K, V>>
    partition(JavaPairRDD<K, V> pairRDD, Function<Tuple2<K, V>, Boolean> predicate) {
        JavaPairRDD<K, V> rddPredTrue = pairRDD.filter(predicate);
        JavaPairRDD<K, V> rddPredFalse = pairRDD.filter(x -> !predicate.call(x));

        return new Tuple2<>(rddPredTrue, rddPredFalse);
    }

    /**
     * Spark needs functions to be serializable, because it has to send them to executors. Trying to pass a non
     * serializable function will result in a TaskNotSerializableException being thrown.
     * Comparators (from java.util.Comparator), by default, aren't serializable, thus we needed to find a workaround.
     * A common solution is explicitly define a static class implementing both Comparator and the Serializable
     * interface, however it's inelegant.
     * This syntax, that uses a lambda expression, is a preferable alternative.
     *
     * For more information about this syntax, refer to: https://stackoverflow.com/a/22808112/6174476
     */
    public static Comparator<Tuple2<String, Long>>
            classCountComparator = (Comparator<Tuple2<String, Long>> & Serializable)(x, y) -> {
        if (x._2() < y._2()) {
            return -1;
        } else if (x._2() > y._2()) {
            return 1;
        }

        // if class count is the same, ties are broken in favor of the smaller class in alphabetical order
        return y._1().compareTo(x._1());
    };
}
