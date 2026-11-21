package io.github.yangzhang1994.guard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * One StockLedger event, as published to the "inventory-events" Kafka topic:
 * {"sku":"SKU-001","version":7,"type":"StockReserved","orderId":"o42","qty":2,"ts":"2026-10-01T08:00:00Z"}
 *
 * <p>(sku, version) is unique in StockLedger's event store, so a repeat can only be a redelivery.
 */
public class InventoryEvent {
    public static final String RECEIVED = "StockReceived";
    public static final String RESERVED = "StockReserved";
    public static final String COMMITTED = "ReservationCommitted";
    public static final String RELEASED = "ReservationReleased";

    private static final ObjectMapper JSON = new ObjectMapper();

    public String sku;
    public long version;
    public String type;
    public String orderId;
    public int qty;
    public long ts; // epoch millis
    /** For commits and releases: time since the matching reservation, or -1 if not known. */
    public long holdMillis = -1;

    public InventoryEvent() {}

    public InventoryEvent(String sku, long version, String type, String orderId, int qty, long ts) {
        this.sku = sku;
        this.version = version;
        this.type = type;
        this.orderId = orderId;
        this.qty = qty;
        this.ts = ts;
    }

    /** Parses one JSON line; throws IOException for anything that is not a well-formed event. */
    public static InventoryEvent parse(String line) throws IOException {
        JsonNode n;
        try {
            n = JSON.readTree(line);
        } catch (IOException e) {
            throw new IOException("not JSON: " + e.getMessage(), e);
        }
        if (n == null || !n.hasNonNull("sku") || !n.hasNonNull("version") || !n.hasNonNull("type")
                || !n.hasNonNull("ts")) {
            throw new IOException("missing sku, version, type or ts");
        }
        String type = n.get("type").asText();
        if (!type.equals(RECEIVED) && !type.equals(RESERVED) && !type.equals(COMMITTED)
                && !type.equals(RELEASED)) {
            throw new IOException("unknown type " + type);
        }
        long ts;
        try {
            ts = Instant.parse(n.get("ts").asText()).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IOException("bad ts " + n.get("ts").asText(), e);
        }
        return new InventoryEvent(
                n.get("sku").asText(),
                n.get("version").asLong(),
                type,
                n.hasNonNull("orderId") ? n.get("orderId").asText() : null,
                n.path("qty").asInt(0),
                ts);
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("{\"sku\":\"").append(sku).append("\",\"version\":").append(version)
                .append(",\"type\":\"").append(type).append('"');
        if (orderId != null) {
            sb.append(",\"orderId\":\"").append(orderId).append('"');
        }
        sb.append(",\"qty\":").append(qty).append(",\"ts\":\"").append(Instant.ofEpochMilli(ts)).append("\"}");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
