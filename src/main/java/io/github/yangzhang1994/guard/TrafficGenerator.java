package io.github.yangzhang1994.guard;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Writes StockLedger-shaped traffic with labelled incidents, so the rules can be scored.
 *
 * <ul>
 *   <li>Normal shoppers: Poisson reservations per SKU (popularity follows a Zipf curve); each is
 *       committed (80%) or released (20%) 20 seconds to 15 minutes later, so some real shoppers
 *       are as quick as the bots. Restocks every 30-60 minutes.
 *   <li>CHURN_BOT (malicious): 4 minutes of reservations that are almost all released within
 *       2 minutes, holding stock away from real buyers.
 *   <li>SCALPER_BURST (malicious): 2 minutes of reservations far above normal, committed fast.
 *   <li>FLASH_SALE (benign): a real promotion, 4-10x the SKU's normal rate for 10-20 minutes.
 * </ul>
 *
 * The output also has what a real topic has: 1% redelivered events, 10% of events up to 20 s late,
 * and a few garbage lines. Labels go to a CSV: kind,sku,start,end.
 *
 * <pre>TrafficGenerator --out data/events.jsonl --labels data/labels.csv --hours 6 --seed 42</pre>
 */
public final class TrafficGenerator {

    private record Raw(long ts, String sku, String type, String orderId, int qty) {}

    private record Line(long emitAt, String text) {}

    public record Label(String kind, String sku, long start, long end) {}

    public static void main(String[] args) throws IOException {
        Map<String, String> o = Job.parse(args);
        Path out = Path.of(o.getOrDefault("out", "data/events.jsonl"));
        Path labels = Path.of(o.getOrDefault("labels", "data/labels.csv"));
        int hours = Integer.parseInt(o.getOrDefault("hours", "6"));
        long seed = Long.parseLong(o.getOrDefault("seed", "42"));
        int perMinute = Integer.parseInt(o.getOrDefault("rate", "300"));
        int[] incidents = {
            Integer.parseInt(o.getOrDefault("churn-bots", "12")),
            Integer.parseInt(o.getOrDefault("scalpers", "8")),
            Integer.parseInt(o.getOrDefault("flash-sales", "6")),
        };
        Stats s = generate(out, labels, hours, seed, perMinute, incidents);
        System.out.printf("wrote %,d lines (%,d events, %,d redelivered, %,d late, %,d garbage) to %s%n",
                s.lines, s.events, s.duplicates, s.late, s.garbage, out);
        System.out.printf("wrote %d labelled incidents to %s%n", s.labels, labels);
    }

    public record Stats(int lines, int events, int duplicates, int late, int garbage, int labels) {}

    public static Stats generate(Path out, Path labelsOut, int hours, long seed, int perMinute, int[] incidents)
            throws IOException {
        Random rnd = new Random(seed);
        long start = Instant.parse("2026-10-01T08:00:00Z").toEpochMilli();
        long minute = 60_000L;
        long end = start + hours * 60 * minute;
        int skuCount = 40;
        String[] skus = new String[skuCount];
        double[] rate = new double[skuCount]; // reservations per minute
        double norm = 0;
        for (int i = 0; i < skuCount; i++) {
            skus[i] = String.format("SKU-%03d", i + 1);
            norm += 1.0 / Math.pow(i + 1, 0.9);
        }
        for (int i = 0; i < skuCount; i++) {
            rate[i] = perMinute * (1.0 / Math.pow(i + 1, 0.9)) / norm;
        }

        List<Raw> raw = new ArrayList<>();
        int[] orderSeq = {0};

        // Restocks.
        for (String sku : skus) {
            for (long t = start + rnd.nextInt(30) * minute; t < end; t += (30 + rnd.nextInt(31)) * minute) {
                raw.add(new Raw(t, sku, InventoryEvent.RECEIVED, null, 100 + rnd.nextInt(201)));
            }
        }

        // Incidents first, so normal traffic can be boosted during flash sales.
        List<Label> labels = new ArrayList<>();
        long earliest = start + 30 * minute; // give baselines time to warm up
        String[] kinds = {"CHURN_BOT", "SCALPER_BURST", "FLASH_SALE"};
        long[] lengths = {4 * minute, 2 * minute, 0};
        for (int k = 0; k < kinds.length; k++) {
            for (int n = 0; n < incidents[k]; n++) {
                int i = rnd.nextInt(skuCount);
                long len = k == 2 ? (10 + rnd.nextInt(11)) * minute : lengths[k];
                long t0 = earliest + (long) (rnd.nextDouble() * (end - earliest - len - 20 * minute));
                t0 -= t0 % 1000;
                labels.add(new Label(kinds[k], skus[i], t0, t0 + len));
            }
        }

        // Normal shoppers (and flash-sale shoppers, who behave the same at a higher rate).
        for (int i = 0; i < skuCount; i++) {
            for (long m = start; m < end; m += minute) {
                double lambda = rate[i];
                for (Label l : labels) {
                    if (l.kind.equals("FLASH_SALE") && l.sku.equals(skus[i]) && m >= l.start && m < l.end) {
                        lambda *= 4 + (l.start / minute % 7); // 4-10x, fixed per sale
                    }
                }
                int n = poisson(rnd, lambda);
                for (int j = 0; j < n; j++) {
                    long t = m + rnd.nextInt(60_000);
                    reservation(raw, rnd, orderSeq, skus[i], t, 0.20, 20_000, 15 * minute);
                }
            }
        }

        // Malicious traffic.
        for (Label l : labels) {
            if (l.kind.equals("CHURN_BOT")) {
                int perMin = 6 + rnd.nextInt(5);
                for (long m = l.start; m < l.end; m += minute) {
                    for (int j = 0; j < perMin; j++) {
                        reservation(raw, rnd, orderSeq, l.sku, m + rnd.nextInt(60_000), 0.90, 30_000, 120_000);
                    }
                }
            } else if (l.kind.equals("SCALPER_BURST")) {
                int perMin = 15 + rnd.nextInt(11);
                for (long m = l.start; m < l.end; m += minute) {
                    for (int j = 0; j < perMin; j++) {
                        reservation(raw, rnd, orderSeq, l.sku, m + rnd.nextInt(60_000), 0.05, 10_000, 40_000);
                    }
                }
            }
        }

        raw.removeIf(r -> r.ts >= end);
        raw.sort(Comparator.comparingLong(Raw::ts));

        // Versions follow event time per SKU, as in StockLedger's store.
        Map<String, Long> versions = new HashMap<>();
        List<Line> lines = new ArrayList<>(raw.size() + raw.size() / 50);
        int duplicates = 0;
        int late = 0;
        for (Raw r : raw) {
            long v = versions.merge(r.sku, 1L, Long::sum);
            String json = new InventoryEvent(r.sku, v, r.type, r.orderId, r.qty, r.ts).toJson();
            long emit = r.ts;
            if (rnd.nextDouble() < 0.10) {
                emit += 1_000 + rnd.nextInt(19_000);
                late++;
            }
            lines.add(new Line(emit, json));
            if (rnd.nextDouble() < 0.01) {
                lines.add(new Line(emit + 1_000 + rnd.nextInt(59_000), json));
                duplicates++;
            }
        }
        int garbage = 0;
        for (int g = 0; g < raw.size() / 5_000; g++) {
            long t = start + (long) (rnd.nextDouble() * (end - start));
            lines.add(new Line(t, g % 2 == 0 ? "{\"sku\":\"SKU-001\",\"type\":\"Stock" : "not json at all"));
            garbage++;
        }
        lines.sort(Comparator.comparingLong(Line::emitAt));

        Files.createDirectories(out.toAbsolutePath().getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out))) {
            for (Line l : lines) {
                w.println(l.text);
            }
        }
        Files.createDirectories(labelsOut.toAbsolutePath().getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(labelsOut))) {
            w.println("kind,sku,start,end");
            for (Label l : labels) {
                w.printf("%s,%s,%s,%s%n", l.kind, l.sku, Instant.ofEpochMilli(l.start), Instant.ofEpochMilli(l.end));
            }
        }
        return new Stats(lines.size(), raw.size(), duplicates, late, garbage, labels.size());
    }

    public static List<Label> readLabels(Path csv) throws IOException {
        List<Label> labels = new ArrayList<>();
        for (String line : Files.readAllLines(csv)) {
            if (line.startsWith("kind,")) {
                continue;
            }
            String[] f = line.split(",");
            labels.add(new Label(f[0], f[1], Instant.parse(f[2]).toEpochMilli(), Instant.parse(f[3]).toEpochMilli()));
        }
        return labels;
    }

    private static void reservation(List<Raw> raw, Random rnd, int[] orderSeq, String sku, long t,
                                    double releaseChance, long minDelay, long maxDelay) {
        String orderId = "o" + (++orderSeq[0]);
        int qty = 1 + rnd.nextInt(3);
        raw.add(new Raw(t, sku, InventoryEvent.RESERVED, orderId, qty));
        long done = t + minDelay + (long) (rnd.nextDouble() * (maxDelay - minDelay));
        String outcome = rnd.nextDouble() < releaseChance ? InventoryEvent.RELEASED : InventoryEvent.COMMITTED;
        raw.add(new Raw(done, sku, outcome, orderId, qty));
    }

    private static int poisson(Random rnd, double lambda) {
        if (lambda > 30) { // normal approximation keeps big rates fast
            return Math.max(0, (int) Math.round(lambda + Math.sqrt(lambda) * rnd.nextGaussian()));
        }
        double l = Math.exp(-lambda);
        double p = 1.0;
        int k = 0;
        do {
            k++;
            p *= rnd.nextDouble();
        } while (p > l);
        return k - 1;
    }
}
