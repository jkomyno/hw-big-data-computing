package hw2.outputstrategy;

import java.time.Duration;

/**
 * Strategy pattern implementation for printing out the information required by the homework:
 * - title of the type of algorithm that run
 * - maximum distance returned by the algorithm
 * - running time in milliseconds
 */
public abstract class OutputStrategy {
    public final String title;
    public final double maxDistance;
    public final Duration runningTime;

    public OutputStrategy(String title, double maxDistance, Duration runningTime) {
        this.title = title;
        this.maxDistance = maxDistance;
        this.runningTime = runningTime;
    }
}
