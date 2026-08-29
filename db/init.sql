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

-- Spark'ın z-skor anomali tespiti (consumer/alerts.py) sonuçları.
-- Aynı DDL consumer açılışında da CREATE IF NOT EXISTS ile koşar (eski volume'lar için).
CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ      NOT NULL,   -- uyarıya konu 1 dk pencere (UTC)
    symbol       TEXT             NOT NULL,
    kind         TEXT             NOT NULL,   -- ret_pct | volume_usd | trade_count
    value        DOUBLE PRECISION NOT NULL,   -- o dakikadaki değer
    baseline     DOUBLE PRECISION NOT NULL,   -- önceki N dk ortalaması
    sigma        DOUBLE PRECISION NOT NULL,   -- önceki N dk standart sapması
    z            DOUBLE PRECISION NOT NULL,   -- (value - baseline) / sigma
    avg_price    DOUBLE PRECISION,
    detected_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    UNIQUE (window_start, symbol, kind)
);
