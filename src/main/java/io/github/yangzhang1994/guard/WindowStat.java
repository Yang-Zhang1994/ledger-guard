package io.github.yangzhang1994.guard;

/** Event counts for one SKU over one event-time window. */
public class WindowStat {
    public String sku;
    public long windowStart;
    public long windowEnd;
    public int received;
    public int reserved;
    public int committed;
    public int released;
    public int unitsReserved;
    /** Releases that came soon after their reservation. */
    public int quickReleased;
    /** Commits that came soon after their reservation. */
    public int quickCommitted;

    public WindowStat() {}

    @Override
    public String toString() {
        return String.format("%s [%d,%d) reserved=%d released=%d (quick %d) committed=%d (quick %d) received=%d",
                sku, windowStart, windowEnd, reserved, released, quickReleased, committed, quickCommitted, received);
    }
}
