package hw2.outputstrategy;

import java.time.Duration;

public class TwoApproxOutputStrategy extends OutputStrategy {
    public TwoApproxOutputStrategy(double maxDistance, Duration runningTime) {
        super("2-APPROXIMATION", maxDistance, runningTime);
    }
}
