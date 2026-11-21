package io.github.yangzhang1994.guard;

import java.io.Serializable;
import java.time.Duration;

/** Thresholds for the pipeline. The defaults are what the evaluation in the README used. */
public class Rules implements Serializable {
    /** How late an event may arrive and still count in its window. */
    public Duration outOfOrderness = Duration.ofSeconds(30);
    /** How long to remember (sku, version) pairs for de-duplication. */
    public Duration dedupeTtl = Duration.ofHours(1);

    /**
     * true: rules on reservation hold time (the current rules). false: the first version, which
     * looked only at volumes (release share and per-minute spikes). Kept so the two can be compared.
     */
    public boolean holdTimeRules = true;
    /** A release this soon after its reservation counts as quick. */
    public Duration quickRelease = Duration.ofMinutes(2);
    /** A commit this soon after its reservation counts as quick. */
    public Duration quickCommit = Duration.ofSeconds(60);
    public double quickShare = 0.5;

    public Duration churnWindow = Duration.ofMinutes(5);
    public Duration churnSlide = Duration.ofMinutes(1);
    public int churnMinReserved = 10;
    public double churnReleaseRatio = 0.6;

    public int spikeMinReserved = 10;
    public double spikeFactor = 6.0;
    /** Weight of the newest minute in the moving-average baseline. */
    public double spikeAlpha = 0.1;
    /** Active minutes a SKU needs before its baseline is trusted. */
    public int spikeWarmupMinutes = 10;
}
