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
    .master("local[*]")              # lokal mod, tüm CPU çekirdeklerini kullan
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")  # Kafka connector
    # Windows'ta state store commit'in .crc hatası vermesini önler (checksum'sız FS)
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    # lokal modda 200 shuffle partition gereksiz; küçük tutmak daha hızlı
    .config("spark.sql.shuffle.partitions", "4")
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

# ---------- 6. Sonucu ekrana yaz (test için) ----------
query = (
    windowed.writeStream
    .outputMode("update")           # pencere güncellenince yaz
    .format("console")              # şimdilik ekrana yaz (sonra DB'ye)
    .option("truncate", False)      # uzun satırları kesme
    .trigger(processingTime="30 seconds")  # 30 saniyede bir güncelle
    .start()
)

print("Spark consumer başladı, Kafka'dan okuyup 1dk pencereler oluşturuyor...")
print("Durdurmak için Ctrl+C")

query.awaitTermination()