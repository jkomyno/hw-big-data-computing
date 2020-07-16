package hw2.outputstrategy;

import java.time.Duration;

public class ExactOutputStrategy extends OutputStrategy {
    public ExactOutputStrategy(double maxDistance, Duration runningTime) {
        super("EXACT", maxDistance, runningTime);
    }
}