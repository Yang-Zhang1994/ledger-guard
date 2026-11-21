package io.github.yangzhang1994.guard;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** JSONEachRow formatting for the two ClickHouse tables in clickhouse/schema.sql. */
public final class Rows {
    private static final DateTimeFormatter CH_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private Rows() {}

    static String time(long epochMillis) {
        return CH_TIME.format(Instant.ofEpochMilli(epochMillis));
    }

    static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    public static String minuteStat(WindowStat s) {
        return "{\"sku\":" + quote(s.sku)
                + ",\"window_start\":" + quote(time(s.windowStart))
                + ",\"window_end\":" + quote(time(s.windowEnd))
                + ",\"received\":" + s.received
                + ",\"reserved\":" + s.reserved
                + ",\"committed\":" + s.committed
                + ",\"released\":" + s.released
                + ",\"units_reserved\":" + s.unitsReserved + "}";
    }

    public static String alert(Alert a) {
        return "{\"sku\":" + quote(a.sku)
                + ",\"rule\":" + quote(a.rule)
                + ",\"window_start\":" + quote(time(a.windowStart))
                + ",\"window_end\":" + quote(time(a.windowEnd))
                + ",\"reserved\":" + a.reserved
                + ",\"released\":" + a.released
                + ",\"score\":" + a.score
                + ",\"detail\":" + quote(a.detail) + "}";
    }
}
