package hw2;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {
    /**
     * Fixed seed for random generators that makes the results reproducible, as per homework specifications.
     */
    public final static long SEED = 20;

    /**
     * Converts a line from the input dataset to a multidimensional vector
     *
     * @param str line
     * @return multidimensional vector containing the coordinates in line
     */
    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    /**
     * Parses the input file and builds a list of multidimensional vectors of coordinates
     *
     * @param filename the input file
     * @return list of multidimensional vectors with the same number of dimensions
     * @throws IllegalArgumentException if the file path is a directory and not a single file
     * @throws IOException              if the provided file can't be read from I/O
     */
    public static List<Vector> readVectorsSeq(String filename) throws IllegalArgumentException, IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }

        // as of Java8, the returned List is guaranteed to have concrete type ArrayList
        return Files.lines(Paths.get(filename))
                .map(Utils::strToVector)
                .collect(Collectors.toList());
    }

    /**
     * Parse the value of K from the command arguments.
     * It must be an integer in [1 <= k <= inputPointsCardinality - 1].
     *
     * @param kStr                   value of K read from the command arguments
     * @param inputPointsCardinality cardinality of inputPoints
     * @return the value k as an integer
     * @throws NumberFormatException if the kStr can't be interpreted as a number in the valid k range
     */
    public static Integer parseK(String kStr, int inputPointsCardinality) throws NumberFormatException {
        try {
            // k must be interpreted as a positive, decimal number
            Integer k = Integer.parseUnsignedInt(kStr, 10);

            // k must be in the range [1 <= k <= inputPointsCardinality - 1]
            if (k < 1 || k >= inputPointsCardinality) {
                throw new MalformedInputException(k);
            }
            return k;
        } catch (Exception e) {
            String errorMsg = "k must be an integer >= 1 and < than the cardinality of inputPoints (%d)";
            throw new NumberFormatException(String.format(errorMsg, inputPointsCardinality));
        }
    }
}
