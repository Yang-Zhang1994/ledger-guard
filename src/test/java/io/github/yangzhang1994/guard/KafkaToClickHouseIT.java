package io.github.yangzhang1994.guard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The whole path against real services: generated traffic is published to Kafka, the job reads
 * it and writes to ClickHouse, and ClickHouse must hold exactly what the same pipeline computes
 * from the file. Then everything is replayed, as after a crash, and the tables must not change.
 */
@Testcontainers
class KafkaToClickHouseIT {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(4).build());

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>("clickhouse/clickhouse-server:24.8")
            .withEnv("CLICKHOUSE_SKIP_USER_SETUP", "1")
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123).withStartupTimeout(Duration.ofMinutes(2)));

    @TempDir
    static Path dir;

    static final HttpClient HTTP = HttpClient.newHttpClient();
    static Path events;
    static Path labels;

    @BeforeAll
    static void setUp() throws Exception {
        events = dir.resolve("events.jsonl");
        labels = dir.resolve("labels.csv");
        TrafficGenerator.generate(events, labels, 2, 7, 120, new int[] {3, 2, 1});
        for (String ddl : Files.readString(Path.of("clickhouse/schema.sql")).split(";")) {
            String sql = ddl.lines().filter(l -> !l.strip().startsWith("--")).reduce("", (a, b) -> a + "\n" + b).strip();
            if (!sql.isEmpty()) {
                clickhouse(sql);
            }
        }
    }

    static String clickhouse(String sql) throws Exception {
        String url = "http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123) + "/";
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(sql)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), r.body());
        return r.body().strip();
    }

    static void runJobFromKafka(String group) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3); // one reader per partition
        Job.build(env, Map.of(
                "kafka", KAFKA.getBootstrapServers(),
                "topic", "inventory-events",
                "group", group,
                "bounded", "true",
                "clickhouse", "http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123)), new Rules());
        env.execute("kafka-to-clickhouse");
    }

    static List<Alert> alertsFromFile() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> file = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                new org.apache.flink.core.fs.Path(events.toString())).build();
        Pipeline.Result r = Pipeline.build(env.fromSource(file, WatermarkStrategy.noWatermarks(), "file"), new Rules());
        CloseableIterator<Alert> it = r.alerts.collectAsync();
        env.execute("file-reference");
        List<Alert> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    @Test
    void kafkaToClickHouseMatchesTheFileRunAndSurvivesAReplay() throws Exception {
        // StockLedger's topic has 3 partitions keyed by SKU.
        KAFKA.execInContainer("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                "--create", "--topic", "inventory-events", "--partitions", "3", "--replication-factor", "1");
        long published = KafkaReplay.replay(KAFKA.getBootstrapServers(), "inventory-events", events);
        assertTrue(published > 10_000);

        runJobFromKafka("first-run");

        List<Alert> expected = alertsFromFile();
        assertTrue(expected.size() >= 3, "generated incidents should raise alerts: " + expected);
        long alerts = Long.parseLong(clickhouse("SELECT count() FROM alerts FINAL"));
        long minutes = Long.parseLong(clickhouse("SELECT count() FROM sku_minute_stats FINAL"));
        assertEquals(expected.size(), alerts);
        assertTrue(minutes > 1_000, "minute rows: " + minutes);
        String bySku = clickhouse("SELECT rule, sku FROM alerts FINAL ORDER BY rule, sku FORMAT CSV");
        for (Alert a : expected) {
            assertTrue(bySku.contains("\"" + a.rule + "\",\"" + a.sku + "\""), a + " missing from\n" + bySku);
        }

        // A second consumer group rereads the whole topic: every row is sent again.
        runJobFromKafka("replay");
        clickhouse("OPTIMIZE TABLE alerts FINAL");
        clickhouse("OPTIMIZE TABLE sku_minute_stats FINAL");
        assertEquals(alerts, Long.parseLong(clickhouse("SELECT count() FROM alerts")));
        assertEquals(minutes, Long.parseLong(clickhouse("SELECT count() FROM sku_minute_stats")));
    }
}
