package hw3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Config {
    public static JavaSparkContext createSparkConfig() {
        // Spark configuration
        SparkConf config = new SparkConf(true)
                .setAppName("Homework3");
        // .set("spark.driver.host", "localhost")
        // .setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(config);
        ctx.setLogLevel("WARN");

        return ctx;
    }
}
