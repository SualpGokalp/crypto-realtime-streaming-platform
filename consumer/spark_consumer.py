import os

# ---------- 0. Ortam değişkenleri ----------
# JAVA_HOME / HADOOP_HOME terminalde ayarlı değilse (ör. env ayarlanmadan önce
# açılmış bir terminal) burada bilinen kurulum yollarına set edilir.
# pyspark import edilmeden ÖNCE yapılmalı, aksi halde JVM başlatılamaz.
_JAVA_HOME_DEFAULT = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
_HADOOP_HOME_DEFAULT = r"C:\hadoop"

if not os.environ.get("JAVA_HOME") and os.path.isdir(_JAVA_HOME_DEFAULT):
    os.environ["JAVA_HOME"] = _JAVA_HOME_DEFAULT
if not os.environ.get("HADOOP_HOME") and os.path.isdir(_HADOOP_HOME_DEFAULT):
    os.environ["HADOOP_HOME"] = _HADOOP_HOME_DEFAULT

# java.exe ve winutils.exe'yi PATH'e ekle
for _home in (os.environ.get("JAVA_HOME"), os.environ.get("HADOOP_HOME")):
    if _home:
        _bin = os.path.join(_home, "bin")
        if _bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = _bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max, count, sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ---------- 1. Spark oturumu ----------
spark = (
    SparkSession.builder
    .appName("CryptoStreamProcessor")
    .master("local[2]")              # 2 çekirdek yeter (dk'da ~10K mesaj); local[*] makineyi kastırıyor
    .config("spark.driver.memory", "1g")    # JVM'i 1 GB ile sınırla
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")  # Kafka connector
    # Windows'ta state store commit'in .crc hatası vermesini önler (checksum'sız FS)
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    # Checkpoint log'u varsayılan olarak FileContext API'sinden (ChecksumFs) geçer ve
    # yukarıdaki ayarı görmez → Windows'ta .crc rename hatası "CONCURRENT_STREAM_LOG_UPDATE"
    # olarak patlar. FileSystem tabanlı yöneticiye zorlayınca o da Raw FS'i kullanır.
    .config("spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
    # lokal modda 200 shuffle partition gereksiz; küçük tutmak daha hızlı
    .config("spark.sql.shuffle.partitions", "4")
    # Binance timestamp'i UTC epoch; oturumu UTC'ye sabitleyince event_time ve
    # pencere sınırları da UTC olur (aksi halde makinenin saat dilimi karışır)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # gereksiz logları azalt

# ---------- 2. Kafka'dan oku ----------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "latest")       # sadece yeni mesajları al
    .load()
)

# ---------- 3. JSON şemasını tanımla ----------
# Producer'ın yazdığı format:
# {"symbol": "BTCUSDT", "price": 58670.95, "quantity": 0.001, "timestamp": 1234567890, "ingested_at": "..."}
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("ingested_at", StringType()),
])

# ---------- 4. JSON'u parse et ----------
parsed_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_time",
        (col("timestamp") / 1000).cast("timestamp")   # ms → saniye → timestamp
    )
)

# ---------- 5. Windowing: 1 dakikalık pencereler ----------
windowed = (
    parsed_stream
    .withWatermark("event_time", "30 seconds")   # geç gelen veriye 30sn tolerans
    .groupBy(
        window(col("event_time"), "1 minute"),   # 1 dakikalık pencere
        col("symbol")
    )
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("trade_count"),
        sum("quantity").alias("total_volume"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("trade_count"),
        col("total_volume"),
    )
)

# ---------- 6. PostgreSQL'e yaz (foreachBatch + upsert) ----------
# Spark'ın hazır JDBC yazıcısı sadece INSERT/overwrite bilir, "varsa güncelle"
# (upsert) yapamaz. outputMode("update") ile aynı pencere birkaç kez gelir
# (dakika dolana kadar sayılar güncellenir) → bu yüzden upsert şart.
# Çözüm: her mikro-batch'i Python'a alıp psycopg2 ile ON CONFLICT ... DO UPDATE.
import psycopg2
from psycopg2.extras import execute_values
from datetime import timezone


def _utc(dt):
    """PySpark collect() timestamp'leri makinenin yerel saatinde *naive* datetime olarak
    verir (session.timeZone=UTC olsa bile). Naive değeri psycopg2'ye verirsek Postgres
    onu UTC sanır → 3 saat kayma. astimezone() naive'i yerel saat kabul edip UTC'ye çevirir."""
    return dt.astimezone(timezone.utc)

PG_DSN = "host=localhost port=5432 dbname=crypto user=crypto password=crypto"

UPSERT_SQL = """
    INSERT INTO price_windows
        (window_start, window_end, symbol, avg_price, min_price, max_price,
         trade_count, total_volume, updated_at)
    VALUES %s
    ON CONFLICT (window_start, symbol) DO UPDATE SET
        window_end   = EXCLUDED.window_end,
        avg_price    = EXCLUDED.avg_price,
        min_price    = EXCLUDED.min_price,
        max_price    = EXCLUDED.max_price,
        trade_count  = EXCLUDED.trade_count,
        total_volume = EXCLUDED.total_volume,
        updated_at   = now()
"""


def write_to_postgres(batch_df, batch_id):
    """Her trigger'da (30 sn) Spark bu fonksiyonu 1 kez çağırır.
    batch_df: o anda değişen pencere satırları (genelde 1-3 satır)."""
    rows = [
        (_utc(r.window_start), _utc(r.window_end), r.symbol, r.avg_price, r.min_price,
         r.max_price, r.trade_count, r.total_volume)
        for r in batch_df.collect()          # satır sayısı küçük → driver'a çekmek güvenli
    ]
    if not rows:
        return
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        execute_values(
            cur, UPSERT_SQL, rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, now())",  # updated_at DB'de üretilir
        )
    print(f"[batch {batch_id}] {len(rows)} pencere satırı Postgres'e yazıldı", flush=True)


query = (
    windowed.writeStream
    .outputMode("update")                  # pencere güncellenince tekrar gönder
    .foreachBatch(write_to_postgres)       # console yerine kendi fonksiyonumuz
    .option("checkpointLocation", "checkpoint/price_windows")  # kaldığı yeri hatırlasın
    .trigger(processingTime="30 seconds")  # 30 saniyede bir
    .start()
)

print("Spark consumer başladı: Kafka -> 1dk pencere -> PostgreSQL (price_windows)")
print("Durdurmak için Ctrl+C")

query.awaitTermination()
