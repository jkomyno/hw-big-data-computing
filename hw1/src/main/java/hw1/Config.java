package hw1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Config {
    public static JavaSparkContext createSparkConfig() {
        // Spark configuration
        SparkConf config = new SparkConf(true)
                .setAppName("Homework1");
        // .set("spark.driver.host", "localhost")
        // .setMaster("local[*]");

        JavaSparkContext ctx = new JavaSparkContext(config);
        ctx.setLogLevel("WARN");

        return ctx;
    }
}
