package io.github.yangzhang1994.guard;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Runs the pipeline.
 *
 * <pre>
 *   --kafka localhost:9092 --topic inventory-events [--bounded]   read StockLedger's topic
 *   --input data/events.jsonl                                     or a file of JSON lines
 *   --clickhouse http://localhost:8123                            write stats and alerts
 *   --print                                                       print alerts to stdout
 * </pre>
 */
public final class Job {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, opts, new Rules());
        env.execute("ledger-guard");
    }

    /** Wires sources and sinks onto env; split out so tests can run the same job. */
    public static Pipeline.Result build(StreamExecutionEnvironment env, Map<String, String> opts, Rules rules) {
        DataStream<String> lines;
        if (opts.containsKey("kafka")) {
            var builder = KafkaSource.<String>builder()
                    .setBootstrapServers(opts.get("kafka"))
                    .setTopics(opts.getOrDefault("topic", "inventory-events"))
                    .setGroupId(opts.getOrDefault("group", "ledger-guard"))
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema());
            if (opts.containsKey("bounded")) {
                builder.setBounded(OffsetsInitializer.latest());
            } else {
                env.enableCheckpointing(10_000); // the ClickHouse sink flushes on each checkpoint
            }
            lines = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "kafka");
        } else if (opts.containsKey("input")) {
            FileSource<String> file = FileSource
                    .forRecordStreamFormat(new TextLineInputFormat(), new Path(opts.get("input")))
                    .build();
            lines = env.fromSource(file, WatermarkStrategy.noWatermarks(), "file");
        } else {
            throw new IllegalArgumentException("pass --kafka <servers> or --input <file>");
        }

        Pipeline.Result result = Pipeline.build(lines, rules);

        if (opts.containsKey("clickhouse")) {
            String url = opts.get("clickhouse");
            result.minuteStats.sinkTo(new ClickHouseSink<>(url, "sku_minute_stats", Rows::minuteStat, 5_000))
                    .name("clickhouse-minute-stats");
            result.alerts.sinkTo(new ClickHouseSink<>(url, "alerts", Rows::alert, 500))
                    .name("clickhouse-alerts");
        }
        if (opts.containsKey("print")) {
            result.alerts.print("alert");
        }
        return result;
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + args[i]);
            }
            String key = args[i].substring(2);
            boolean flag = i + 1 >= args.length || args[i + 1].startsWith("--");
            opts.put(key, flag ? "true" : args[++i]);
        }
        return opts;
    }
}
