package io.github.yangzhang1994.guard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Raw JSON lines -> parsed, de-duplicated events -> per-SKU window counts -> rule alerts.
 *
 * <pre>
 * lines ─ parse ─┬─ malformed (side output)
 *                └─ watermarks ─ keyBy(sku) ─ dedupe ─┬─ duplicates (side output)
 *                                                     └─ hold time (replay in event-time order, match outcome to reservation)
 *                                                          ├─ 1-min tumbling counts ── minuteStats (to ClickHouse)
 *                                                          └─ 5-min sliding counts ─┬─ churn rule ─────────┐
 *                                                                                   └─ fast-checkout rule ─┴─ alerts
 * </pre>
 *
 * With {@link Rules#holdTimeRules} off, the first version's volume-only rules run instead.
 */
public final class Pipeline {
    public static final OutputTag<String> MALFORMED = new OutputTag<>("malformed") {};
    public static final OutputTag<InventoryEvent> DUPLICATES = new OutputTag<>("duplicates") {};

    private Pipeline() {}

    public static final class Result {
        public final DataStream<WindowStat> minuteStats;
        public final DataStream<Alert> alerts;
        public final DataStream<String> malformed;
        public final DataStream<InventoryEvent> duplicates;

        Result(DataStream<WindowStat> minuteStats, DataStream<Alert> alerts,
               DataStream<String> malformed, DataStream<InventoryEvent> duplicates) {
            this.minuteStats = minuteStats;
            this.alerts = alerts;
            this.malformed = malformed;
            this.duplicates = duplicates;
        }
    }

    public static Result build(DataStream<String> lines, Rules rules) {
        SingleOutputStreamOperator<InventoryEvent> parsed = lines.process(new Parse()).name("parse");

        DataStream<InventoryEvent> timed = parsed.assignTimestampsAndWatermarks(
                WatermarkStrategy.<InventoryEvent>forBoundedOutOfOrderness(rules.outOfOrderness)
                        .withTimestampAssigner((e, previous) -> e.ts)
                        .withIdleness(Duration.ofMinutes(1)));

        SingleOutputStreamOperator<InventoryEvent> unique = timed
                .keyBy(e -> e.sku)
                .process(new Deduplicate(rules.dedupeTtl))
                .name("dedupe");

        DataStream<InventoryEvent> withHold = unique
                .keyBy(e -> e.sku)
                .process(new HoldTime(rules.dedupeTtl))
                .name("hold-time");

        DataStream<WindowStat> minuteStats = withHold
                .keyBy(e -> e.sku)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountEvents(rules), new AttachWindow())
                .name("minute-counts");

        DataStream<WindowStat> slidingStats = withHold
                .keyBy(e -> e.sku)
                .window(SlidingEventTimeWindows.of(rules.churnWindow, rules.churnSlide))
                .aggregate(new CountEvents(rules), new AttachWindow())
                .name("sliding-counts");

        DataStream<Alert> alerts;
        if (rules.holdTimeRules) {
            DataStream<Alert> churn = slidingStats.keyBy(s -> s.sku)
                    .process(new ShareRule(Alert.CHURN, Metric.QUICK_RELEASED, rules.churnMinReserved, rules.quickShare, rules.churnWindow))
                    .name("churn-rule");
            DataStream<Alert> fast = slidingStats.keyBy(s -> s.sku)
                    .process(new ShareRule(Alert.FAST_CHECKOUT, Metric.QUICK_COMMITTED, rules.churnMinReserved, rules.quickShare, rules.churnWindow))
                    .name("fast-checkout-rule");
            alerts = churn.union(fast);
        } else {
            DataStream<Alert> churn = slidingStats.keyBy(s -> s.sku)
                    .process(new ShareRule(Alert.CHURN, Metric.RELEASED, rules.churnMinReserved, rules.churnReleaseRatio, rules.churnWindow))
                    .name("churn-rule-v1");
            DataStream<Alert> spike = minuteStats.keyBy(s -> s.sku).process(new SpikeRule(rules)).name("spike-rule-v1");
            alerts = churn.union(spike);
        }

        return new Result(minuteStats, alerts, parsed.getSideOutput(MALFORMED), unique.getSideOutput(DUPLICATES));
    }

    /** Bad lines go to a side output instead of failing the job. */
    static final class Parse extends ProcessFunction<String, InventoryEvent> {
        @Override
        public void processElement(String line, Context ctx, Collector<InventoryEvent> out) {
            try {
                out.collect(InventoryEvent.parse(line));
            } catch (Exception e) {
                ctx.output(MALFORMED, line);
            }
        }
    }

    /** Kafka delivers at least once; drop a (sku, version) pair seen before. */
    static final class Deduplicate extends KeyedProcessFunction<String, InventoryEvent, InventoryEvent> {
        private final Duration ttl;
        private transient MapState<Long, Boolean> seen;

        Deduplicate(Duration ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(OpenContext ctx) {
            MapStateDescriptor<Long, Boolean> d = new MapStateDescriptor<>("seen-versions", Long.class, Boolean.class);
            d.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
            seen = getRuntimeContext().getMapState(d);
        }

        @Override
        public void processElement(InventoryEvent e, Context ctx, Collector<InventoryEvent> out) throws Exception {
            if (seen.contains(e.version)) {
                ctx.output(DUPLICATES, e);
                return;
            }
            seen.put(e.version, Boolean.TRUE);
            out.collect(e);
        }
    }

    /**
     * Gives each commit and release the time since its reservation.
     *
     * <p>Events for a SKU can arrive out of order (late Kafka records, or reordering between
     * parallel operators), and a release seen before its reservation would have no hold time. So
     * events are buffered and replayed in event-time order from timers: a timer for time t fires
     * once the watermark passes t, when every event up to t that is not late has arrived. Each
     * replayed record keeps its own timestamp because a timer emits at the timer's time.
     */
    static final class HoldTime extends KeyedProcessFunction<String, InventoryEvent, InventoryEvent> {
        private final Duration ttl;
        private transient MapState<Long, List<InventoryEvent>> pending;
        private transient MapState<String, Long> reservedAt;

        HoldTime(Duration ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(OpenContext ctx) {
            pending = getRuntimeContext().getMapState(new MapStateDescriptor<>("pending",
                    Types.LONG, Types.LIST(Types.POJO(InventoryEvent.class))));
            MapStateDescriptor<String, Long> d = new MapStateDescriptor<>("reserved-at", String.class, Long.class);
            d.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
            reservedAt = getRuntimeContext().getMapState(d);
        }

        @Override
        public void processElement(InventoryEvent e, Context ctx, Collector<InventoryEvent> out) throws Exception {
            List<InventoryEvent> atTs = pending.get(e.ts);
            if (atTs == null) {
                atTs = new ArrayList<>();
                ctx.timerService().registerEventTimeTimer(e.ts);
            }
            atTs.add(e);
            pending.put(e.ts, atTs);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<InventoryEvent> out) throws Exception {
            List<InventoryEvent> atTs = pending.get(ts);
            if (atTs == null) {
                return;
            }
            pending.remove(ts);
            atTs.sort(Comparator.comparingLong(ev -> ev.version)); // same millisecond: store order
            for (InventoryEvent e : atTs) {
                if (e.orderId != null) {
                    if (e.type.equals(InventoryEvent.RESERVED)) {
                        reservedAt.put(e.orderId, e.ts);
                    } else {
                        Long at = reservedAt.get(e.orderId);
                        if (at != null) {
                            e.holdMillis = e.ts - at;
                            reservedAt.remove(e.orderId);
                        }
                    }
                }
                out.collect(e);
            }
        }
    }

    static final class CountEvents implements AggregateFunction<InventoryEvent, WindowStat, WindowStat> {
        private final long quickRelease;
        private final long quickCommit;

        CountEvents(Rules rules) {
            this.quickRelease = rules.quickRelease.toMillis();
            this.quickCommit = rules.quickCommit.toMillis();
        }

        @Override
        public WindowStat createAccumulator() {
            return new WindowStat();
        }

        @Override
        public WindowStat add(InventoryEvent e, WindowStat acc) {
            switch (e.type) {
                case InventoryEvent.RECEIVED -> acc.received++;
                case InventoryEvent.RESERVED -> {
                    acc.reserved++;
                    acc.unitsReserved += e.qty;
                }
                case InventoryEvent.COMMITTED -> {
                    acc.committed++;
                    if (e.holdMillis >= 0 && e.holdMillis <= quickCommit) {
                        acc.quickCommitted++;
                    }
                }
                case InventoryEvent.RELEASED -> {
                    acc.released++;
                    if (e.holdMillis >= 0 && e.holdMillis <= quickRelease) {
                        acc.quickReleased++;
                    }
                }
                default -> { }
            }
            return acc;
        }

        @Override
        public WindowStat getResult(WindowStat acc) {
            return acc;
        }

        @Override
        public WindowStat merge(WindowStat a, WindowStat b) {
            a.received += b.received;
            a.reserved += b.reserved;
            a.committed += b.committed;
            a.released += b.released;
            a.unitsReserved += b.unitsReserved;
            a.quickReleased += b.quickReleased;
            a.quickCommitted += b.quickCommitted;
            return a;
        }
    }

    static final class AttachWindow extends ProcessWindowFunction<WindowStat, WindowStat, String, TimeWindow> {
        @Override
        public void process(String sku, Context ctx, Iterable<WindowStat> counts, Collector<WindowStat> out) {
            WindowStat s = counts.iterator().next();
            s.sku = sku;
            s.windowStart = ctx.window().getStart();
            s.windowEnd = ctx.window().getEnd();
            out.collect(s);
        }
    }

    /** Which count a {@link ShareRule} compares with the window's reservations. */
    enum Metric {
        RELEASED("released"),
        QUICK_RELEASED("released within the quick-release limit"),
        QUICK_COMMITTED("committed within the quick-commit limit");

        final String label;

        Metric(String label) {
            this.label = label;
        }

        int of(WindowStat w) {
            return switch (this) {
                case RELEASED -> w.released;
                case QUICK_RELEASED -> w.quickReleased;
                case QUICK_COMMITTED -> w.quickCommitted;
            };
        }
    }

    /**
     * Fires when a window has enough reservations and a large enough share of them ended a given
     * way. Overlapping sliding windows see the same burst several times, so after an alert the
     * rule stays quiet for one more window length: a burst that straddles two windows is one
     * incident, not two.
     */
    static final class ShareRule extends KeyedProcessFunction<String, WindowStat, Alert> {
        private final String rule;
        private final Metric metric;
        private final int minReserved;
        private final double share;
        private final long cooldown;
        private transient ValueState<Long> quietUntil;

        ShareRule(String rule, Metric metric, int minReserved, double share, Duration cooldown) {
            this.rule = rule;
            this.metric = metric;
            this.minReserved = minReserved;
            this.share = share;
            this.cooldown = cooldown.toMillis();
        }

        @Override
        public void open(OpenContext ctx) {
            quietUntil = getRuntimeContext().getState(new ValueStateDescriptor<>(rule + "-quiet-until", Long.class));
        }

        @Override
        public void processElement(WindowStat w, Context ctx, Collector<Alert> out) throws Exception {
            if (w.reserved < minReserved) {
                return;
            }
            int count = metric.of(w);
            double ratio = (double) count / w.reserved;
            if (ratio < share) {
                return;
            }
            Long quiet = quietUntil.value();
            if (quiet != null && w.windowStart < quiet) {
                return;
            }
            quietUntil.update(w.windowEnd + cooldown);
            out.collect(new Alert(w.sku, rule, w, ratio,
                    String.format("%d of %d reservations %s", count, w.reserved, metric.label)));
        }
    }

    /**
     * Compares each active minute with an exponentially weighted average of the SKU's earlier
     * minutes. Quiet minutes produce no window, so the average is decayed for the gap first. A
     * minute that fires is not folded into the average, so an attack cannot raise its own bar.
     */
    static final class SpikeRule extends KeyedProcessFunction<String, WindowStat, Alert> {
        private static final long MINUTE = 60_000L;
        private final Rules rules;
        private transient ValueState<Double> baseline;
        private transient ValueState<Long> lastEnd;
        private transient ValueState<Integer> activeMinutes;

        SpikeRule(Rules rules) {
            this.rules = rules;
        }

        @Override
        public void open(OpenContext ctx) {
            baseline = getRuntimeContext().getState(new ValueStateDescriptor<>("spike-baseline", Double.class));
            lastEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("spike-last-end", Long.class));
            activeMinutes = getRuntimeContext().getState(new ValueStateDescriptor<>("spike-active", Integer.class));
        }

        @Override
        public void processElement(WindowStat w, Context ctx, Collector<Alert> out) throws Exception {
            Double avg = baseline.value();
            Long prevEnd = lastEnd.value();
            int seen = activeMinutes.value() == null ? 0 : activeMinutes.value();
            if (avg != null && prevEnd != null && w.windowStart > prevEnd) {
                long quietMinutes = (w.windowStart - prevEnd) / MINUTE;
                avg *= Math.pow(1 - rules.spikeAlpha, quietMinutes);
            }

            boolean fired = false;
            if (avg != null && seen >= rules.spikeWarmupMinutes && w.reserved >= rules.spikeMinReserved) {
                double bar = Math.max(avg, 0.5) * rules.spikeFactor;
                if (w.reserved >= bar) {
                    fired = true;
                    out.collect(new Alert(w.sku, Alert.SPIKE, w, w.reserved / Math.max(avg, 0.5),
                            String.format("%d reservations vs usual %.1f/min", w.reserved, avg)));
                }
            }

            if (!fired) {
                avg = avg == null ? w.reserved : rules.spikeAlpha * w.reserved + (1 - rules.spikeAlpha) * avg;
            }
            baseline.update(avg);
            lastEnd.update(w.windowEnd);
            activeMinutes.update(seen + 1);
        }
    }
}
