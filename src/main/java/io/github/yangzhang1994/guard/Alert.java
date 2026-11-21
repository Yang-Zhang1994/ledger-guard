package io.github.yangzhang1994.guard;

/** A rule firing for one SKU over one window. */
public class Alert {
    /** Many reservations that mostly get released: bots holding stock so real buyers cannot. */
    public static final String CHURN = "RESERVE_RELEASE_CHURN";
    /** Many reservations checked out within seconds: scalper bots buying out stock. */
    public static final String FAST_CHECKOUT = "FAST_CHECKOUT_BURST";
    /** First-version rule: reservations in one minute far above the SKU's own recent rate. */
    public static final String SPIKE = "RESERVATION_SPIKE";

    public String sku;
    public String rule;
    public long windowStart;
    public long windowEnd;
    public int reserved;
    public int released;
    public double score;
    public String detail;

    public Alert() {}

    public Alert(String sku, String rule, WindowStat w, double score, String detail) {
        this.sku = sku;
        this.rule = rule;
        this.windowStart = w.windowStart;
        this.windowEnd = w.windowEnd;
        this.reserved = w.reserved;
        this.released = w.released;
        this.score = score;
        this.detail = detail;
    }

    @Override
    public String toString() {
        return String.format("%s %s [%d,%d) score=%.2f %s", rule, sku, windowStart, windowEnd, score, detail);
    }
}
