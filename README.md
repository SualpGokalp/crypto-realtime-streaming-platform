# Crypto Real-Time Streaming Platform

Binance'ten canlı kripto işlem verisini **Kafka** üzerinden alıp **Apache Spark Structured Streaming** ile 1 dakikalık pencerelerde toplayan gerçek zamanlı veri işleme hattı.

## Mimari

```
Binance WebSocket  ──►  Kafka (topic: crypto-prices)  ──►  Spark Structured Streaming  ──►  Konsol
   (producer)            (docker-compose)                     (consumer, 1dk pencere)
```

- **Producer** (`producer/binance_producer.py`): Binance `btcusdt@trade` websocket akışını dinler, her işlemi Kafka'ya yazar.
- **Kafka** (`docker-compose.yml`): Tek node KRaft modda broker + Kafka UI (`localhost:8081`).
- **Consumer** (`consumer/spark_consumer.py`): Kafka'dan okur, event-time üzerinden 1 dakikalık pencerelerde `avg / min / max / count / sum` hesaplar (30sn watermark).

## Gereksinimler

- Python 3.10+
- Java (JDK 17) — Spark için
- Docker Desktop — Kafka için

## Kurulum

```bash
pip install -r requirements.txt
```

Windows'ta Spark Structured Streaming için `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) gerekir ve `HADOOP_HOME` ayarlanmalıdır. Consumer, `JAVA_HOME`/`HADOOP_HOME` ayarlı değilse bilinen kurulum yollarını kendisi denemektedir.

## Çalıştırma

```bash
# 1) Kafka'yı başlat
docker compose up -d kafka

# 2) Producer (bir terminalde)
python producer/binance_producer.py

# 3) Consumer (başka bir terminalde)
python consumer/spark_consumer.py
```

## Örnek Çıktı

```
|window_start       |window_end         |symbol |avg_price|min_price|max_price|trade_count|total_volume|
|2026-07-01 17:59:00|2026-07-01 18:00:00|BTCUSDT|59538.80 |59536.99 |59542.00 |260        |1.478       |
```
