-- Spark consumer'ın 1 dakikalık pencere sonuçlarını yazdığı tablo.
-- Postgres container'ı ilk kez ayağa kalkarken otomatik çalışır
-- (docker-entrypoint-initdb.d). Sonraki açılışlarda veri volume'da kalır.
--
-- (window_start, symbol) birincil anahtar: aynı pencere tekrar geldiğinde
-- (outputMode=update) yeni satır eklenmez, consumer ON CONFLICT ile günceller.
CREATE TABLE IF NOT EXISTS price_windows (
    window_start  TIMESTAMPTZ      NOT NULL,   -- pencere başlangıcı (UTC)
    window_end    TIMESTAMPTZ      NOT NULL,
    symbol        TEXT             NOT NULL,   -- BTCUSDT, ETHUSDT, ...
    avg_price     DOUBLE PRECISION,
    min_price     DOUBLE PRECISION,
    max_price     DOUBLE PRECISION,
    trade_count   BIGINT,
    total_volume  DOUBLE PRECISION,
    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),  -- son upsert zamanı
    PRIMARY KEY (window_start, symbol)
);
