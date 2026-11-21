package io.github.yangzhang1994.guard;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * Writes rows to ClickHouse over its HTTP interface as JSONEachRow, in batches.
 *
 * <p>Delivery is at least once: the writer flushes on every checkpoint, so after a restart some
 * rows may be sent again. The tables are ReplacingMergeTree keyed on the window, which collapses
 * those repeats.
 */
public class ClickHouseSink<T> implements Sink<T> {

    /** Turns a record into one JSON object line matching the table's columns. */
    @FunctionalInterface
    public interface RowFormatter<T> extends Serializable {
        String toJsonRow(T record);
    }

    private final String baseUrl;
    private final String table;
    private final RowFormatter<T> formatter;
    private final int batchSize;

    public ClickHouseSink(String baseUrl, String table, RowFormatter<T> formatter, int batchSize) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.table = table;
        this.formatter = formatter;
        this.batchSize = batchSize;
    }

    @Override
    @SuppressWarnings("deprecation") // Flink 1.20 still requires this overload
    public SinkWriter<T> createWriter(InitContext context) {
        return new Writer();
    }

    private final class Writer implements SinkWriter<T> {
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        private final List<String> rows = new ArrayList<>();
        private final URI insertUri = URI.create(baseUrl + "/?query="
                + URLEncoder.encode("INSERT INTO " + table + " FORMAT JSONEachRow", StandardCharsets.UTF_8));

        @Override
        public void write(T element, Context context) throws IOException, InterruptedException {
            rows.add(formatter.toJsonRow(element));
            if (rows.size() >= batchSize) {
                send();
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            send();
        }

        @Override
        public void close() throws Exception {
            send();
        }

        private void send() throws IOException, InterruptedException {
            if (rows.isEmpty()) {
                return;
            }
            String body = String.join("\n", rows);
            IOException last = null;
            for (int attempt = 1; attempt <= 3; attempt++) {
                HttpRequest request = HttpRequest.newBuilder(insertUri)
                        .timeout(Duration.ofSeconds(30))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                try {
                    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        rows.clear();
                        return;
                    }
                    last = new IOException("ClickHouse " + response.statusCode() + ": " + response.body().strip());
                    if (response.statusCode() < 500) {
                        break; // a bad row or schema will not fix itself on retry
                    }
                } catch (IOException e) {
                    last = e;
                }
                Thread.sleep(200L * attempt);
            }
            throw last;
        }
    }
}
