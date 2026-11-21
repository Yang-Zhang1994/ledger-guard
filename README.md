# LedgerGuard

A Flink (Java) job that watches [StockLedger](https://github.com/Yang-Zhang1994/stockledger)'s
inventory events in real time and flags two kinds of bot abuse, writing per-minute statistics
and alerts to ClickHouse.

- **Reserve-and-release churn:** bots reserve stock and release it within a couple of minutes,
  over and over, so real buyers see it as sold out.
- **Fast-checkout bursts:** scalper bots reserve and pay within seconds, buying out stock.

```
StockLedger ──► Kafka "inventory-events" (keyed by SKU) ──► Flink job ──┬─► ClickHouse sku_minute_stats
                                                                        └─► ClickHouse alerts
```

## Inside the job

```
parse ─┬─ garbage lines ──► side output (job keeps running)
       └─ event-time watermarks (30 s for late events)
            └─ keyBy(sku) ─ de-duplicate (sku, version) ─┬─ redeliveries ──► side output
                 └─ hold time: replay each SKU's events in event-time order and
                    match every commit/release to its reservation by orderId
                      ├─ 1-minute tumbling counts ─────────────────► sku_minute_stats
                      └─ 5-minute sliding counts (1-minute slide)
                           ├─ churn rule:          ≥10 reserved and ≥50% released within 2 min ─┐
                           └─ fast-checkout rule:  ≥10 reserved and ≥50% committed within 60 s ──┴► alerts
```

| Concern | How it is handled |
|---|---|
| Kafka delivers at least once | Keyed state remembers each SKU's `(sku, version)` pairs (1-hour TTL) and drops repeats |
| Events arrive late or out of order | Event-time windows with a 30-second watermark bound. The hold-time step buffers events and replays them from event-time timers, so a release is never seen before its reservation |
| Bad input | Unparseable lines go to a side output instead of failing the job |
| Overlapping sliding windows | After an alert, a rule stays quiet for one window length, so one burst is one alert |
| Writing to ClickHouse | A custom Flink `Sink` batches rows to ClickHouse's HTTP interface (JSONEachRow) and flushes on every checkpoint. Delivery is at least once; the tables are `ReplacingMergeTree` keyed on the window, so a replay does not double-count |

## Results

`TrafficGenerator` writes six hours of StockLedger-shaped traffic for 40 SKUs (Zipf popularity,
~110,000 reservations) with 26 labelled incidents: 12 churn bots, 8 scalper bursts, and
6 **legitimate flash sales** the rules should leave alone. Like a real topic, it also has 1%
redelivered events, 10% late events (up to 20 s), and some garbage lines. `Evaluate` runs the
pipeline over it and scores every alert against the labels.

The first version of the rules looked only at volume: the share of reservations released, and
reservations per minute against the SKU's moving average. The second version uses **how long each
reservation was held**, which needs the stateful reservation-to-outcome join above.

| | v1: volume only | v2: hold time |
|---|---|---|
| Alerts | 75 | 18 |
| Alerts on real incidents (precision) | 15 (**20%**) | 18 (**100%**) |
| Churn bots caught | 10 of 12 | **11 of 12** |
| Scalper bursts caught | 3 of 8 | **8 of 8** |
| Legitimate flash sales flagged | 6 of 6 | **0 of 6** |
| Median time from incident start to first alert | churn 5.4 min, scalper 1.1 min | churn 5.0 min, scalper 2.2 min |

Why v1 failed: ordinary cart abandonment (20% of shoppers) piles up after every flash sale and
looks like churn, and a flash sale's volume spike looks exactly like a scalper's. Hold time
separates them: real shoppers usually take minutes, bots take seconds.

The same data sent through Kafka and Flink into ClickHouse gives the same 18 alerts. On an Apple M4
laptop (local Flink, parallelism 10) the pipeline processes the 222,341 lines in about 2.6 s, about
**86,000 events per second**, and drops exactly the 2,279 redeliveries and 44 garbage lines the
generator injected.

```sql
SELECT rule, sku, formatDateTime(window_start, '%H:%i') AS from, formatDateTime(window_end, '%H:%i') AS to,
       reserved, round(score, 2) AS share, detail
FROM alerts FINAL ORDER BY window_start LIMIT 3;
```
```
┌─rule──────────────────┬─sku─────┬─from──┬─to────┬─reserved─┬─share─┬─detail────────────────────────────────────────────────────────┐
│ FAST_CHECKOUT_BURST   │ SKU-010 │ 08:30 │ 08:35 │       78 │   0.5 │ 39 of 78 reservations committed within the quick-commit limit │
│ FAST_CHECKOUT_BURST   │ SKU-029 │ 09:15 │ 09:20 │       49 │  0.65 │ 32 of 49 reservations committed within the quick-commit limit │
│ RESERVE_RELEASE_CHURN │ SKU-029 │ 09:24 │ 09:29 │       37 │  0.57 │ 21 of 37 reservations released within the quick-release limit │
└───────────────────────┴─────────┴───────┴───────┴──────────┴───────┴───────────────────────────────────────────────────────────────┘
```

### Limits

- **The data is synthetic.** The generator gives bots short hold times, so 100% precision here
  shows the pipeline works and why v1 failed, not how it would score on real traffic. Some real
  shoppers in the generator are as quick as bots, but real traffic would be messier.
- **Per-SKU totals can hide a bot.** The missed churn bot was on a busier SKU: its quick releases
  were 25 of 67 reservations in the window, under the 50% bar. Catching it needs features per
  customer, device or IP, which StockLedger's events do not carry.

## Run it

Requires Java 17, Maven and Docker.

```bash
mvn test        # 17 unit tests on a Flink mini cluster
mvn verify      # plus the Kafka -> Flink -> ClickHouse test in Docker (Testcontainers)

# generate traffic and score both rule versions
mvn -q compile
mvn -q exec:exec -Dmain=io.github.yangzhang1994.guard.TrafficGenerator -Dargs="--out data/events.jsonl --labels data/labels.csv"
mvn -q exec:exec -Dmain=io.github.yangzhang1994.guard.Evaluate -Dargs="--events data/events.jsonl --labels data/labels.csv"
mvn -q exec:exec -Dmain=io.github.yangzhang1994.guard.Evaluate -Dargs="--events data/events.jsonl --labels data/labels.csv --rules v1"

# the full path: Kafka -> Flink -> ClickHouse
docker compose up -d
mvn -q exec:exec -Dmain=io.github.yangzhang1994.guard.KafkaReplay -Dargs="--file data/events.jsonl"
mvn -q exec:exec -Dmain=io.github.yangzhang1994.guard.Job -Dargs="--kafka localhost:9092 --topic inventory-events --bounded --clickhouse http://localhost:8123"
curl -s localhost:8123/ --data-binary "SELECT rule, count() FROM alerts FINAL GROUP BY rule"
```

Without `--bounded` the job keeps reading the topic and checkpoints every 10 seconds.

## Tests

| Suite | What it checks |
|---|---|
| `InventoryEventTest` | Parsing StockLedger events, rejecting bad lines, JSON round trip, ClickHouse row format |
| `PipelineTest` | On a Flink mini cluster: redeliveries counted once; a late event lands in its own minute; garbage goes to the side output; quick and slow outcomes are told apart; a churn bot raises one alert despite overlapping windows; cart abandonment is not churn (v1 flagged it); a scalper burst is caught; a flash sale is left alone (v1's spike rule flagged it) |
| `KafkaToClickHouseIT` | Real Kafka and ClickHouse in Docker: ClickHouse holds exactly the alerts the pipeline computes from the file, and replaying the whole topic leaves the tables unchanged |

GitHub Actions and the Jenkinsfile run `mvn verify` on every push.

Two bugs the tests found while building it: a release could reach the hold-time step before its
reservation (records for one SKU reordered between parallel operators), which made results vary
between runs, fixed by replaying events from event-time timers; and a burst straddling two
windows raised two alerts, fixed with the cooldown.
