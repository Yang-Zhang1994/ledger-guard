package io.github.yangzhang1994.guard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PipelineTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

    private static final long T0 = Instant.parse("2026-10-01T08:00:00Z").toEpochMilli();

    /** Builds the lines for one SKU in order, numbering versions as StockLedger would. */
    private static final class Events {
        final List<String> lines = new ArrayList<>();
        final Map<String, Long> versions = new HashMap<>();
        int order;

        String add(String sku, String type, String orderId, long atSeconds) {
            long v = versions.merge(sku, 1L, Long::sum);
            String line = new InventoryEvent(sku, v, type, orderId, 1, T0 + atSeconds * 1000).toJson();
            lines.add(line);
            return line;
        }

        /** One reservation and its outcome holdSeconds later. */
        void reservation(String sku, long atSeconds, String outcome, long holdSeconds) {
            String id = "o" + (++order);
            add(sku, InventoryEvent.RESERVED, id, atSeconds);
            add(sku, outcome, id, atSeconds + holdSeconds);
        }

        /** Lines sorted by event time, as a well-behaved topic would deliver them. */
        List<String> sorted() {
            return lines.stream()
                    .sorted((a, b) -> Long.compare(ts(a), ts(b)))
                    .collect(Collectors.toList());
        }
    }

    private static long ts(String line) {
        try {
            return InventoryEvent.parse(line).ts;
        } catch (Exception e) {
            return 0;
        }
    }

    private record Output(List<WindowStat> minutes, List<Alert> alerts, List<String> malformed, List<InventoryEvent> duplicates) {
        List<Alert> rule(String name) {
            return alerts.stream().filter(a -> a.rule.equals(name)).toList();
        }

        WindowStat minute(String sku, long startSeconds) {
            return minutes.stream()
                    .filter(m -> m.sku.equals(sku) && m.windowStart == T0 + startSeconds * 1000)
                    .findFirst().orElseThrow();
        }
    }

    private static Output run(List<String> lines, Rules rules) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Pipeline.Result r = Pipeline.build(env.fromData(lines).setParallelism(1), rules);
        CloseableIterator<WindowStat> m = r.minuteStats.collectAsync();
        CloseableIterator<Alert> a = r.alerts.collectAsync();
        CloseableIterator<String> bad = r.malformed.collectAsync();
        CloseableIterator<InventoryEvent> dup = r.duplicates.collectAsync();
        env.execute("pipeline-test");
        return new Output(drain(m), drain(a), drain(bad), drain(dup));
    }

    private static <T> List<T> drain(CloseableIterator<T> it) {
        List<T> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    @Test
    void redeliveredEventIsCountedOnce() throws Exception {
        Events e = new Events();
        String line = e.add("SKU-1", InventoryEvent.RESERVED, "o1", 10);
        List<String> lines = new ArrayList<>(e.lines);
        lines.add(line);

        Output out = run(lines, new Rules());

        assertEquals(1, out.minute("SKU-1", 0).reserved);
        assertEquals(1, out.duplicates.size());
    }

    @Test
    void lateEventWithinTheBoundLandsInItsOwnMinute() throws Exception {
        Events e = new Events();
        String early = e.add("SKU-1", InventoryEvent.RESERVED, "o1", 50); // 08:00:50
        String later = e.add("SKU-1", InventoryEvent.RESERVED, "o2", 70); // 08:01:10

        Output out = run(List.of(later, early), new Rules()); // arrives 20 s late

        assertEquals(1, out.minute("SKU-1", 0).reserved);
        assertEquals(1, out.minute("SKU-1", 60).reserved);
    }

    @Test
    void garbageLinesGoToTheSideOutputAndTheJobKeepsRunning() throws Exception {
        Events e = new Events();
        e.add("SKU-1", InventoryEvent.RESERVED, "o1", 5);
        List<String> lines = new ArrayList<>(List.of("not json", "{\"sku\":\"SKU-1\",\"type\":\"Stock"));
        lines.addAll(e.lines);

        Output out = run(lines, new Rules());

        assertEquals(2, out.malformed.size());
        assertEquals(1, out.minute("SKU-1", 0).reserved);
    }

    @Test
    void holdTimeSeparatesQuickAndSlowOutcomes() throws Exception {
        Events e = new Events();
        e.reservation("SKU-1", 0, InventoryEvent.RELEASED, 30);   // released in 30 s: quick
        e.reservation("SKU-1", 5, InventoryEvent.COMMITTED, 20);  // committed in 20 s: quick
        e.reservation("SKU-1", 10, InventoryEvent.RELEASED, 600); // released after 10 min: not quick

        Output out = run(e.sorted(), new Rules());

        WindowStat first = out.minute("SKU-1", 0);
        assertEquals(1, first.quickReleased);
        assertEquals(1, first.quickCommitted);
        assertEquals(0, out.minute("SKU-1", 600).quickReleased);
    }

    @Test
    void churnBotRaisesOneAlertDespiteOverlappingWindows() throws Exception {
        Events e = new Events();
        for (int i = 0; i < 15; i++) {
            e.reservation("SKU-BOT", i * 12L, InventoryEvent.RELEASED, 60); // 15 holds over 3 min
        }

        List<Alert> churn = run(e.sorted(), new Rules()).rule(Alert.CHURN);

        assertEquals(1, churn.size(), churn.toString());
        assertTrue(churn.get(0).score >= 0.5);
    }

    @Test
    void ordinaryCartAbandonmentIsNotChurn() throws Exception {
        Events e = new Events();
        for (int i = 0; i < 80; i++) { // 20 minutes of steady shoppers who leave after 8 minutes
            e.reservation("SKU-1", i * 15L, InventoryEvent.RELEASED, 480);
        }
        List<String> lines = e.sorted();

        assertEquals(0, run(lines, new Rules()).rule(Alert.CHURN).size());

        Rules v1 = new Rules();
        v1.holdTimeRules = false; // the first version only counted releases, so it fired here
        assertTrue(run(lines, v1).rule(Alert.CHURN).size() >= 1);
    }

    @Test
    void scalperBurstIsCaughtByFastCheckout() throws Exception {
        Events e = new Events();
        for (int i = 0; i < 20; i++) {
            e.reservation("SKU-HOT", i * 6L, InventoryEvent.COMMITTED, 15);
        }

        assertEquals(1, run(e.sorted(), new Rules()).rule(Alert.FAST_CHECKOUT).size());
    }

    @Test
    void flashSaleIsLeftAloneButTrippedTheOldSpikeRule() throws Exception {
        Events e = new Events();
        for (int m = 0; m < 15; m++) { // a quiet quarter hour builds the baseline
            e.reservation("SKU-SALE", m * 60L + 5, InventoryEvent.COMMITTED, 300);
        }
        for (int i = 0; i < 40; i++) { // then a promotion: 40 shoppers in a minute, normal checkout times
            e.reservation("SKU-SALE", 900 + i, InventoryEvent.COMMITTED, 180 + i * 5L);
        }
        List<String> lines = e.sorted();

        assertEquals(List.of(), run(lines, new Rules()).alerts);

        Rules v1 = new Rules();
        v1.holdTimeRules = false;
        assertEquals(1, run(lines, v1).rule(Alert.SPIKE).size());
    }
}
