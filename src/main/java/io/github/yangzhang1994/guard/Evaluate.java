package io.github.yangzhang1994.guard;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/**
 * Runs the pipeline over a generated file and scores the alerts against its labels.
 *
 * <p>An alert is a true positive if its window overlaps a malicious incident on the same SKU, a
 * "flash sale" false positive if it overlaps a benign promotion, and noise otherwise.
 *
 * <pre>Evaluate --events data/events.jsonl --labels data/labels.csv [--rules v1]</pre>
 */
public final class Evaluate {

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Job.parse(args);
        Path events = Path.of(o.getOrDefault("events", "data/events.jsonl"));
        List<TrafficGenerator.Label> labels = TrafficGenerator.readLabels(Path.of(o.getOrDefault("labels", "data/labels.csv")));
        long lineCount;
        try (var stream = Files.lines(events)) {
            lineCount = stream.count();
        }

        Rules rules = new Rules();
        rules.holdTimeRules = !"v1".equals(o.get("rules"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Pipeline.Result r = Pipeline.build(env.fromSource(
                org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
                        new org.apache.flink.connector.file.src.reader.TextLineInputFormat(),
                        new org.apache.flink.core.fs.Path(events.toString())).build(),
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "file"), rules);
        CloseableIterator<Alert> alertIt = r.alerts.collectAsync();
        CloseableIterator<String> malformedIt = r.malformed.collectAsync();
        CloseableIterator<InventoryEvent> dupIt = r.duplicates.collectAsync();

        long t0 = System.nanoTime();
        env.executeAsync("ledger-guard-evaluate");
        List<Alert> alerts = new ArrayList<>();
        alertIt.forEachRemaining(alerts::add);
        long malformed = count(malformedIt);
        long duplicates = count(dupIt);
        double seconds = (System.nanoTime() - t0) / 1e9;

        System.out.printf("Input: %,d lines in %.1f s = %,.0f events/s (parallelism %d)%n",
                lineCount, seconds, lineCount / seconds, env.getParallelism());
        System.out.printf("Dropped %,d redelivered events and %,d garbage lines%n%n", duplicates, malformed);

        List<String> ruleNames = rules.holdTimeRules ? List.of(Alert.CHURN, Alert.FAST_CHECKOUT) : List.of(Alert.CHURN, Alert.SPIKE);
        System.out.println("Rules: " + (rules.holdTimeRules ? "hold time (v2)" : "volume only (v1)") + "\n");
        System.out.println("| Rule | Alerts | On attacks | On flash sales | Other | Precision |");
        System.out.println("|---|---|---|---|---|---|");
        for (String rule : ruleNames) {
            int total = 0, hit = 0, flash = 0;
            for (Alert a : alerts) {
                if (!a.rule.equals(rule)) {
                    continue;
                }
                total++;
                String kind = matchKind(a, labels);
                if (kind == null) {
                    continue;
                }
                if (kind.equals("FLASH_SALE")) {
                    flash++;
                } else {
                    hit++;
                }
            }
            System.out.printf("| %s | %d | %d | %d | %d | %s |%n", rule, total, hit, flash, total - hit - flash,
                    total == 0 ? "-" : String.format("%.0f%%", 100.0 * hit / total));
        }

        System.out.println();
        System.out.println("| Incident | Count | Alerted on | By rule | Median time to first alert |");
        System.out.println("|---|---|---|---|---|");
        for (String kind : List.of("CHURN_BOT", "SCALPER_BURST", "FLASH_SALE")) {
            int n = 0, caught = 0;
            List<Long> delays = new ArrayList<>();
            java.util.Set<String> firedRules = new java.util.TreeSet<>();
            List<String> missed = new ArrayList<>();
            for (TrafficGenerator.Label l : labels) {
                if (!l.kind().equals(kind)) {
                    continue;
                }
                n++;
                long first = Long.MAX_VALUE;
                for (Alert a : alerts) {
                    if (overlaps(a, l)) {
                        first = Math.min(first, a.windowEnd);
                        firedRules.add(a.rule);
                    }
                }
                if (first != Long.MAX_VALUE) {
                    caught++;
                    delays.add(first - l.start());
                } else {
                    missed.add(l.sku());
                }
            }
            delays.sort(null);
            String median = delays.isEmpty() ? "-" : String.format("%.1f min", delays.get(delays.size() / 2) / 60_000.0);
            System.out.printf("| %s | %d | %d%s | %s | %s |%n", kind, n, caught,
                    missed.isEmpty() ? "" : " (missed " + String.join(", ", missed) + ")",
                    firedRules.isEmpty() ? "-" : String.join(", ", firedRules), median);
        }
    }

    static boolean overlaps(Alert a, TrafficGenerator.Label l) {
        // Allow one minute after the incident: its last reservations close the next window.
        return a.sku.equals(l.sku()) && a.windowStart < l.end() + 60_000 && a.windowEnd > l.start();
    }

    static String matchKind(Alert a, List<TrafficGenerator.Label> labels) {
        String benign = null;
        for (TrafficGenerator.Label l : labels) {
            if (overlaps(a, l)) {
                if (!l.kind().equals("FLASH_SALE")) {
                    return l.kind();
                }
                benign = l.kind();
            }
        }
        return benign;
    }

    private static long count(CloseableIterator<?> it) {
        long n = 0;
        while (it.hasNext()) {
            it.next();
            n++;
        }
        return n;
    }
}
