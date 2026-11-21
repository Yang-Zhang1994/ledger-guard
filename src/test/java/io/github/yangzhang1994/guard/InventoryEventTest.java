package io.github.yangzhang1994.guard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InventoryEventTest {

    @Test
    void parsesAStockLedgerEvent() throws IOException {
        InventoryEvent e = InventoryEvent.parse(
                "{\"sku\":\"SKU-001\",\"version\":7,\"type\":\"StockReserved\",\"orderId\":\"o42\",\"qty\":2,\"ts\":\"2026-10-01T08:00:00Z\"}");
        assertEquals("SKU-001", e.sku);
        assertEquals(7, e.version);
        assertEquals("o42", e.orderId);
        assertEquals(2, e.qty);
        assertEquals(1790841600000L, e.ts);
        assertEquals(-1, e.holdMillis);
    }

    @Test
    void restocksHaveNoOrder() throws IOException {
        InventoryEvent e = InventoryEvent.parse(
                "{\"sku\":\"SKU-001\",\"version\":1,\"type\":\"StockReceived\",\"qty\":50,\"ts\":\"2026-10-01T08:00:00Z\"}");
        assertNull(e.orderId);
    }

    @Test
    void toJsonRoundTrips() throws IOException {
        InventoryEvent e = new InventoryEvent("SKU-9", 3, InventoryEvent.RELEASED, "o1", 1, 1790841600123L);
        InventoryEvent back = InventoryEvent.parse(e.toJson());
        assertEquals(e.toJson(), back.toJson());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "not json",
        "{\"sku\":\"SKU-001\",\"type\":\"Stock",
        "{\"sku\":\"SKU-001\",\"version\":1,\"type\":\"StockStolen\",\"ts\":\"2026-10-01T08:00:00Z\"}",
        "{\"sku\":\"SKU-001\",\"version\":1,\"type\":\"StockReserved\",\"ts\":\"yesterday\"}",
        "{\"version\":1,\"type\":\"StockReserved\",\"ts\":\"2026-10-01T08:00:00Z\"}",
    })
    void rejectsBadLines(String line) {
        assertThrows(IOException.class, () -> InventoryEvent.parse(line));
    }

    @Test
    void clickHouseRowsUseUtcTimesAndEscapeText() {
        Alert a = new Alert();
        a.sku = "SKU-1";
        a.rule = Alert.CHURN;
        a.windowStart = 1790841600000L;
        a.windowEnd = 1790841900000L;
        a.detail = "say \"hi\"";
        assertEquals("{\"sku\":\"SKU-1\",\"rule\":\"RESERVE_RELEASE_CHURN\",\"window_start\":\"2026-10-01 08:00:00.000\","
                + "\"window_end\":\"2026-10-01 08:05:00.000\",\"reserved\":0,\"released\":0,\"score\":0.0,"
                + "\"detail\":\"say \\\"hi\\\"\"}", Rows.alert(a));
    }
}
