# Crypto Real-Time Streaming Platform

Binance'ten canlı kripto işlem verisini **Kafka** üzerinden alıp **Apache Spark Structured Streaming** ile 1 dakikalık pencerelerde toplayan, sonuçları **PostgreSQL**'e yazan, akış içinde **z-skor anomali uyarısı** üreten ve **Streamlit** paneliyle görselleştiren uçtan uca gerçek zamanlı veri hattı.

## Mimari

```
Binance WebSocket ──► Kafka ──► Spark Structured Streaming ──┬─► PostgreSQL (price_windows) ──► Streamlit paneli
 BTC / ETH / SOL     crypto-prices   1 dk pencere, 30 sn watermark │       ▲
 (producer)          (docker)        2 sorgu: update + append      │       │ geçmiş 30 dk
                                                                   └─► z-skor anomali ──► Kafka crypto-alerts
                                                                       (kesinleşen pencere)   + PostgreSQL alerts
```

| Bileşen | Dosya | Ne yapar |
|---|---|---|
| Producer | `producer/binance_producer.py` | Binance *combined stream* ile `btcusdt/ethusdt/solusdt@trade` akışını tek websocket'ten dinler, her işlemi JSON olarak Kafka `crypto-prices` topic'ine yazar. Bağlantı koparsa 5 sn sonra yeniden bağlanır. |
| Kafka + Postgres | `docker-compose.yml` | Tek node KRaft Kafka (`localhost:9094`), Kafka UI (`localhost:8081`), PostgreSQL 16 (`localhost:5432`, `crypto/crypto`). |
| Şema | `db/init.sql` | `price_windows` (pencere sonuçları) ve `alerts` (uyarılar) tabloları. Volume ilk oluşturulurken çalışır. |
| Consumer | `consumer/spark_consumer.py` | Kafka'dan okur, event-time üzerinden 1 dk pencerede `avg/min/max/count/sum` hesaplar. **Sorgu 1 (`update`)**: pencere her değiştiğinde `foreachBatch` + psycopg2 ile Postgres'e *upsert*. **Sorgu 2 (`append`)**: pencere kesinleşince anomali kontrolü. |
| Anomali | `consumer/alerts.py` | Kesinleşen pencereyi önceki 30 dakikayla karşılaştırır: getiri %, işlem sayısı, dolar hacmi için `z = (x − ort) / σ`. `|z| ≥ 2` → Kafka `crypto-alerts` (key = sembol) + Postgres `alerts`. |
| Panel | `dashboard/app.py` | Endeks, korelasyon, SMA/EMA/VWAP, Bollinger, oynaklık, hacim, z-skor tablosu ve Spark uyarıları; her grafiğin altında hesap açıklaması. |

## Neden iki Spark sorgusu?

- `update` modu pencere **dakika dolmadan** da satır üretir (sayılar kısmi) → panel canlı görsün diye Postgres'e bu modda yazılır, aynı pencere `ON CONFLICT ... DO UPDATE` ile güncellenir.
- `append` modu pencereyi **yalnızca watermark geçince** (kesinleşince) tek sefer verir → z-skoru yarım pencereye uygulayıp sahte alarm üretmemek için anomali sorgusu bu modda çalışır. Tespit gecikmesi tipik olarak 30-90 sn (watermark + trigger).
- Geçmiş (önceki 30 dk) Postgres'ten okunur; yani veritabanı hem *sink* hem *state*. Consumer yeniden başlasa da geçmiş kaybolmaz, `UNIQUE (window_start, symbol, kind)` sayesinde aynı uyarı iki kez yazılmaz.

## Gereksinimler

- Python 3.10+ (proje `.venv` ile)
- JDK 17 (Spark için) — consumer `JAVA_HOME` ayarlı değilse bilinen kurulum yolunu dener
- Windows'ta `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) ve `HADOOP_HOME=C:\hadoop`
- Docker Desktop (Kafka, Postgres)
- `pyspark` **3.5.6**'da pinli: 4.x ile Kafka connector uyumsuz

```bash
pip install -r requirements.txt
```

## Çalıştırma

```powershell
# 1) Altyapı (Kafka UI'yi gerekmedikçe açma: ~400 MB RAM)
docker compose up -d kafka postgres

# 2) Producer
.venv\Scripts\python producer\binance_producer.py

# 3) Consumer (pencere + anomali; ilk açılışta Kafka connector jar'ını indirir)
.venv\Scripts\python consumer\spark_consumer.py

# 4) Panel → http://localhost:8504
.venv\Scripts\python -m streamlit run dashboard\app.py --server.port 8504
```

Anomali eşiği/geçmiş penceresi ortam değişkeniyle ayarlanır (test için eşiği düşürmek pratik):

```powershell
$env:ALERT_Z = "1.0"; $env:ALERT_HISTORY = "30"; .venv\Scripts\python consumer\spark_consumer.py
```

Uyarıları Kafka'dan doğrudan izlemek için:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto-alerts --from-beginning
```

## Veri modeli

**`price_windows`** — `(window_start, symbol)` birincil anahtar

```
window_start        | window_end | symbol  | avg_price | min_price | max_price | trade_count | total_volume | updated_at
2026-08-28 18:59+00 | 19:00+00   | BTCUSDT | 59538.80  | 59536.99  | 59542.00  | 260         | 1.478        | ...
```

**`alerts`** — `UNIQUE (window_start, symbol, kind)`

```
window_start | symbol  | kind        | value  | baseline | sigma | z     | avg_price | detected_at
19:03+00     | SOLUSDT | ret_pct     | -0.412 | 0.011    | 0.118 | -3.58 | 186.42    | 19:04:31+00
19:03+00     | SOLUSDT | trade_count | 1840   | 612.3    | 210.7 | +5.83 | 186.42    | 19:04:31+00
```

Kafka `crypto-alerts` mesajı aynı alanları JSON olarak taşır (`label` alanı Türkçe açıklama: *fiyat sıçraması / hacim patlaması / işlem sayısı patlaması*).

## Notlar

- Spark oturumu `local[2]`, driver 1 GB, `spark.sql.shuffle.partitions=4`, saat dilimi UTC: pencere sınırları Binance timestamp'iyle aynı dilimde olur, panel TR saatine çevirir.
- Windows'ta state store / checkpoint `.crc` hataları için `RawLocalFileSystem` + `FileSystemBasedCheckpointFileManager` ayarları gerekir (consumer içinde açıklamalı).
- Her sorgunun kendi checkpoint klasörü var (`checkpoint/price_windows`, `checkpoint/alerts`); silinirse sorgu `startingOffsets=latest` ile yeniden başlar.
- Panel `st.tabs` yerine `segmented_control` kullanır: gizli sekmeler de render edildiği için 3 kat yük oluyordu.
