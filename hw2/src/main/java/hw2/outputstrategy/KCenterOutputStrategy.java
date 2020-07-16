package hw2.outputstrategy;

import java.time.Duration;

public class KCenterOutputStrategy extends OutputStrategy {
    public KCenterOutputStrategy(double maxDistance, Duration runningTime) {
        super("k-CENTER-BASED", maxDistance, runningTime);
    }
}
