package hw2;

import java.time.Duration;

/**
 * Stopwatch utility to track the execution time of each method.
 * A Stopwatch can be used only once.
 */
public class Stopwatch {
    final long startTime;
    boolean expired;

    private Stopwatch() {
        this.startTime = System.nanoTime();
    }

    Duration stop() {
        Duration result = Duration.ofNanos(System.nanoTime() - this.startTime);
        if (this.expired) {
            throw new UnsupportedOperationException("This stopwatch has already been used!");
        }
        this.expired = true;
        return result;
    }

    /**
     * Factory creator of Stopwatch class
     */
    static public Stopwatch createStarted() {
        return new Stopwatch();
    }
}