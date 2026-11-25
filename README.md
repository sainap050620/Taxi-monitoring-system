# 🚖 Real-Time Taxi Fleet Monitoring Engine

A streaming data platform that replays GPS traces of **10,000+ taxis** and turns them into live fleet metrics: current and average speed, active taxis, over-speeding alerts, geofence violations and completed trips, shown on a **Kibana** dashboard that refreshes every few seconds.

*Built as a team of 4 for the **Big Data** module at TU Hamburg.*

## 🏗️ Architecture

```
GPS trace replay ──► Kafka topic "taxi" ──► Apache Flink (PyFlink) ──► Kafka result topics ──► Logstash ──► Elasticsearch ──► Kibana
 (Python producer)                          │                                                                          dashboard
                                            └── Redis (per-taxi state)
```

| Service | Role |
|---|---|
| **Kafka producer** (`data_provider/`) | Reads the raw GPS log and replays it into Kafka in event-time order, with a configurable fast-forward factor |
| **Kafka + Zookeeper** | Message broker; topics `taxi`, `active_taxi`, `metrics`, `over_speeding`, `avg_speed`, `ended_taxi` |
| **Flink app** (`flink_app/`) | Event-time stream processing with watermarks; writes results back to Kafka |
| **Redis** | Keeps the latest state per taxi between events |
| **Logstash connector** (`kafka_elasticsearch_connector/`) | Routes each result topic into its own Elasticsearch index |
| **Elasticsearch + Kibana** | Storage, search and the live dashboard |

## ⚙️ Stream Processing Operators

| Operator | What it computes |
|---|---|
| `preprocess` | Cleans the stream and drops physically impossible jumps (> 300 km/h) |
| `calculate_distance` | Distance between consecutive GPS points (haversine formula) |
| `calculate_speed` | Current speed per taxi; flags **over-speeding** above 50 km/h |
| `calculate_avg_speed` | Running average speed per taxi |
| `active_taxi` | Keeps taxis within 15 km of the Forbidden City (farther ones are dropped) and flags those 10 to 15 km out as geofence violations |
| `ended_taxi` | Counts completed trips |

## 🚀 Run It

**Prerequisites:** Docker with the Compose plugin, `curl` and `unzip`. About 6 GB of free memory.

```bash
git clone https://github.com/sainap050620/Taxi-monitoring-system.git
cd Taxi-monitoring-system
./scripts/setup_demo.sh      # downloads the dataset sample and installs the dashboard
docker compose up -d
```

`setup_demo.sh` downloads a sample of the public [T-Drive dataset](https://www.microsoft.com/research/publication/t-drive-trajectory-data-sample/) from Microsoft Research (551 taxis, about 8 MB, Beijing, February 2008) and saves it as `volumes/kafka-producer_volume/taxi_log_2008_by_id.zip`. It also copies [`kibana/exported_objects.ndjson`](kibana/exported_objects.ndjson) into place so the connector imports the dashboard on start.

To use the full dataset instead, zip the taxi `.txt` files under that same name in `volumes/kafka-producer_volume/` before starting.

The first start builds the images, so give it a few minutes. Then open the live dashboard:

```
http://localhost:5601/app/dashboards#/view/taxi-fleet-monitor
```

The Flink web UI is available at `http://localhost:8081`.

Stop everything with:

```bash
docker compose down
```

## 🎬 Demo

> **Why there is no hosted link:** the platform needs Kafka, Zookeeper, Flink, Redis, Logstash, Elasticsearch and Kibana running together, which does not fit on any free hosting tier. It runs locally with Docker Compose (see [Run It](#-run-it)).

**What you see once it is running** (Kibana dashboard, refreshing every 5 seconds). The live map is the centrepiece:

| Panel | Shows |
|---|---|
| **Live taxi map** | Every taxi that reported in the last 5 minutes, plotted on a map of Beijing: blue for normal, red for geofence violations (10 to 15 km from the Forbidden City). Refreshes every 5 seconds |
| Active taxis | Unique taxis within 15 km of the Forbidden City that are currently reporting |
| Geofence violations | Unique active taxis that are 10 to 15 km from the Forbidden City |
| Over-speeding events | Number of readings above 50 km/h, plus a timeline of alerts |
| Total distance driven | Cumulative distance of the whole fleet in km |
| Completed trips | Running count of ended trips |
| Average speed over time | Fleet-wide current speed as the stream is processed |
| Highest average speed | Top 10 taxis by running average speed |

The producer replays Beijing taxi traces from February 2008 faster than real time, so a few minutes of runtime covers hours of fleet activity. Watch the Flink job process the stream at `http://localhost:8081`.

![Live Kibana dashboard](docs/demo.gif)

*Recorded from a real run on a 551-taxi sample of the dataset (one of the T-Drive sample archives). Taxis move on the map and the counters update live as the stream is processed. The map's background tiles come from Elastic Maps Service, so the browser needs internet access.*

## 🛠️ Tech Stack

Python · Apache Kafka · Apache Flink (PyFlink) · Redis · Logstash · Elasticsearch · Kibana · Docker Compose

## 📄 License

MIT License, see [LICENSE](LICENSE).
