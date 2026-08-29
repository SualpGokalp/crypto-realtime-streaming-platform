"""
Anomali (z-skor) tespiti — Spark tarafı.

Paneldeki (dashboard/app.py) z-skor hesabının aynısı, ama artık akış içinde:
Spark bir 1 dk penceresini KESİNLEŞTİRİP (watermark geçince, append modu) verdiğinde
bu modül o pencereyi önceki N dakikayla karşılaştırır ve olağan dışıysa

    1) Kafka `crypto-alerts` topic'ine JSON uyarı basar  (başka servisler tüketebilsin)
    2) PostgreSQL `alerts` tablosuna yazar               (panel gösterebilsin)

Formül (her seri için ayrı):  z = (x_şimdi − ort(önceki N dk)) / σ(önceki N dk)
Seriler: dakikalık getiri %, işlem sayısı, dolar hacmi.
Ortalama/σ o dakikayı İÇERMEZ; yoksa aykırı değer kendi eşiğini şişirir.

"Geçmiş" nereden geliyor?  price_windows tablosundan (ilk sorgu oraya yazıyor).
Yani Postgres'i hem sink hem de "durum (state)" olarak kullanıyoruz — Spark'ın kendi
stateful API'sini (applyInPandasWithState) kullanmaktan çok daha basit ve yeniden
başlatmaya dayanıklı: consumer kapanıp açılsa da geçmiş kaybolmaz.
"""
import json
import os
import statistics
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaProducer

PG_DSN = "host=localhost port=5432 dbname=crypto user=crypto password=crypto"
KAFKA_BOOTSTRAP = "localhost:9094"
ALERT_TOPIC = "crypto-alerts"

# Ayarlar ortam değişkeninden okunur; test ederken eşiği düşürmek için pratik:
#   $env:ALERT_Z = "0.5"; python consumer/spark_consumer.py
Z_THRESHOLD = float(os.environ.get("ALERT_Z", "2.0"))     # |z| bu değeri geçince uyarı
HISTORY_MIN = int(os.environ.get("ALERT_HISTORY", "30"))  # geriye kaç dakika bakılır
MIN_HISTORY = 5                                            # bundan az geçmişle z hesaplama (σ güvenilmez)

# Uyarı türleri: kolon adı → panel açıklaması
KINDS = {
    "ret_pct":     "fiyat sıçraması",
    "volume_usd":  "hacim patlaması",
    "trade_count": "işlem sayısı patlaması",
}

# Tablo: consumer her açılışta CREATE IF NOT EXISTS çalıştırır, çünkü db/init.sql
# yalnızca volume ilk kez oluşturulurken koşar (eski volume'da alerts tablosu yok).
DDL = """
CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ      NOT NULL,   -- uyarıya konu 1 dk pencere (UTC)
    symbol       TEXT             NOT NULL,
    kind         TEXT             NOT NULL,   -- ret_pct | volume_usd | trade_count
    value        DOUBLE PRECISION NOT NULL,   -- o dakikadaki değer
    baseline     DOUBLE PRECISION NOT NULL,   -- önceki N dk ortalaması
    sigma        DOUBLE PRECISION NOT NULL,   -- önceki N dk standart sapması
    z            DOUBLE PRECISION NOT NULL,   -- (value - baseline) / sigma
    avg_price    DOUBLE PRECISION,            -- o dakikadaki fiyat (bağlam için)
    detected_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    UNIQUE (window_start, symbol, kind)       -- aynı uyarı iki kez yazılmasın (yeniden başlatma)
);
"""

_producer = None


def _kafka():
    """KafkaProducer'ı ilk ihtiyaçta bir kez kur (her batch'te yeniden bağlanmamak için)."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    return _producer


def ensure_table():
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(DDL)


def _history(cur, symbol, window_start):
    """Bu pencereden ÖNCEKİ en fazla HISTORY_MIN pencere, eskiden yeniye sıralı.
    Zaman sınırı var: consumer uzun süre kapalı kaldıysa günler öncesinin
    verisiyle karşılaştırılmasın."""
    cur.execute(
        """
        SELECT avg_price, trade_count, total_volume
        FROM price_windows
        WHERE symbol = %s
          AND window_start <  %s
          AND window_start >= %s - (%s || ' minutes')::interval
        ORDER BY window_start
        """,
        (symbol, window_start, window_start, HISTORY_MIN + 1),
    )
    return cur.fetchall()


def _series(prices, trades, volumes):
    """Ham listelerden üç seri üret. Getiri = ardışık dakika fiyat değişimi (%)."""
    ret = [(p1 - p0) / p0 * 100 for p0, p1 in zip(prices, prices[1:])]
    vol_usd = [v * p for v, p in zip(volumes, prices)]
    return {"ret_pct": ret, "trade_count": trades, "volume_usd": vol_usd}


def score_window(cur, row):
    """Kesinleşmiş tek bir pencere için uyarı listesi döndürür (boş olabilir)."""
    hist = _history(cur, row.symbol, row.window_start)
    if len(hist) < MIN_HISTORY:
        return []

    prices = [h[0] for h in hist] + [row.avg_price]
    trades = [h[1] for h in hist] + [row.trade_count]
    volumes = [h[2] for h in hist] + [row.total_volume]
    series = _series(prices, trades, volumes)

    alerts = []
    for kind, values in series.items():
        past, current = values[:-1], values[-1]       # kendisini geçmişe dahil etme
        if len(past) < MIN_HISTORY - 1:
            continue
        mu = statistics.fmean(past)
        sd = statistics.pstdev(past)
        if sd == 0:
            continue
        z = (current - mu) / sd
        if abs(z) >= Z_THRESHOLD:
            alerts.append({
                "window_start": row.window_start,
                "symbol": row.symbol,
                "kind": kind,
                "label": KINDS[kind],
                "value": round(current, 6),
                "baseline": round(mu, 6),
                "sigma": round(sd, 6),
                "z": round(z, 3),
                "avg_price": row.avg_price,
                "detected_at": datetime.now(timezone.utc),
            })
    return alerts


def publish(alerts):
    """Uyarıları Kafka'ya (key=symbol → aynı sembol aynı partition'a) ve Postgres'e yaz."""
    if not alerts:
        return
    prod = _kafka()
    for a in alerts:
        prod.send(ALERT_TOPIC, key=a["symbol"], value=a)
    prod.flush()

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO alerts (window_start, symbol, kind, value, baseline, sigma, z, avg_price, detected_at)
            VALUES %s
            ON CONFLICT (window_start, symbol, kind) DO NOTHING
            """,
            [(a["window_start"], a["symbol"], a["kind"], a["value"], a["baseline"],
              a["sigma"], a["z"], a["avg_price"], a["detected_at"]) for a in alerts],
        )


def detect(rows):
    """foreachBatch'ten çağrılır: kesinleşmiş pencere satırları → uyarılar (yazılır ve döndürülür)."""
    found = []
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        for r in rows:
            found.extend(score_window(cur, r))
    publish(found)
    return found
