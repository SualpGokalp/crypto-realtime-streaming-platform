"""Veri katmanı: PostgreSQL sorguları ve türetilmiş gösterge hesapları.

Panelin tek veri kaynağı Spark'ın yazdığı iki tablodur: `price_windows`
(1 dk pencereleri) ve `alerts` (akış içi z-skor uyarıları). Buradaki tüm
fonksiyonlar cache'lidir; TTL'ler Spark'ın yazma temposuna (30 sn) göre seçildi.
"""
import numpy as np
import pandas as pd
import psycopg2
import streamlit as st

PG_DSN = "host=localhost port=5432 dbname=crypto user=crypto password=crypto"
TZ = "Europe/Istanbul"


@st.cache_data(ttl=10)
def load(minutes: int) -> pd.DataFrame:
    with psycopg2.connect(PG_DSN) as conn:
        df = pd.read_sql(
            """
            SELECT window_start, symbol, avg_price, min_price, max_price,
                   trade_count, total_volume, updated_at
            FROM price_windows
            WHERE window_start >= now() - (%s || ' minutes')::interval
            ORDER BY symbol, window_start
            """,
            conn, params=(minutes,),
        )
    df["t"] = df["window_start"].dt.tz_convert(TZ)   # görüntüleme için TR saati
    return df


@st.cache_data(ttl=10)
def load_alerts(minutes: int) -> pd.DataFrame:
    """Spark'ın akış içinde ürettiği uyarılar (consumer/alerts.py → alerts tablosu)."""
    with psycopg2.connect(PG_DSN) as conn:
        try:
            return pd.read_sql(
                """
                SELECT window_start, symbol, kind, value, baseline, sigma, z, avg_price, detected_at
                FROM alerts
                WHERE window_start >= now() - (%s || ' minutes')::interval
                ORDER BY window_start DESC, symbol
                """,
                conn, params=(minutes,),
            )
        except psycopg2.errors.UndefinedTable:      # consumer hiç yeni sürümle açılmadıysa tablo yoktur
            return pd.DataFrame()


@st.cache_data(ttl=300)
def load_profile() -> pd.DataFrame:
    """Günün saatine göre aktivite profili: son 7 günün tüm pencereleri, TR saatiyle
    saat başına ortalama işlem sayısı ve $ hacim. Seçili aralıktan bağımsız (daha uzun)."""
    with psycopg2.connect(PG_DSN) as conn:
        return pd.read_sql(
            """
            SELECT symbol,
                   EXTRACT(HOUR FROM window_start AT TIME ZONE 'Europe/Istanbul')::int AS hour,
                   AVG(trade_count)             AS trades,
                   AVG(total_volume * avg_price) AS volume_usd,
                   COUNT(*)                     AS n
            FROM price_windows
            WHERE window_start >= now() - interval '7 days'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            conn,
        )


@st.cache_data(ttl=10)
def enrich(df: pd.DataFrame, sma_short: int, sma_long: int, bb_n: int, z_n: int) -> pd.DataFrame:
    """Her sembol için türetilmiş göstergeleri hesaplar. Tüm pencereler dakika cinsinden."""
    out = []
    for sym, g in df.groupby("symbol", sort=False):
        g = g.sort_values("window_start").copy()
        p = g["avg_price"]
        g["volume_usd"] = g["total_volume"] * p
        g["avg_trade_usd"] = g["volume_usd"] / g["trade_count"].replace(0, np.nan)
        g["ret_pct"] = p.pct_change() * 100                       # dakikalık getiri
        g["range_pct"] = (g["max_price"] - g["min_price"]) / p * 100  # dakika içi salınım
        g["sma_s"] = p.rolling(sma_short, min_periods=1).mean()
        g["sma_l"] = p.rolling(sma_long, min_periods=1).mean()
        g["ema_s"] = p.ewm(span=sma_short, adjust=False).mean()
        bb_mid = p.rolling(bb_n, min_periods=2).mean()
        bb_sd = p.rolling(bb_n, min_periods=2).std()
        g["bb_mid"], g["bb_up"], g["bb_lo"] = bb_mid, bb_mid + 2 * bb_sd, bb_mid - 2 * bb_sd
        g["vol_pct"] = g["ret_pct"].rolling(z_n, min_periods=2).std()   # oynaklık
        g["index100"] = p / p.iloc[0] * 100                        # ilk dakikaya endeks
        g["cum_ret_pct"] = g["index100"] - 100
        # VWAP: hacim ağırlıklı ortalama fiyat (seçili aralık boyunca kümülatif)
        g["vwap"] = (p * g["total_volume"]).cumsum() / g["total_volume"].cumsum()
        # z-skorlar: "bu dakika normalden ne kadar sapıyor?"
        for col, zcol in (("ret_pct", "z_ret"), ("trade_count", "z_trades"), ("volume_usd", "z_vol")):
            mu = g[col].rolling(z_n, min_periods=3).mean().shift(1)   # kendisini dahil etme
            sd = g[col].rolling(z_n, min_periods=3).std().shift(1)
            g[zcol] = (g[col] - mu) / sd.replace(0, np.nan)
        out.append(g)
    return pd.concat(out) if out else df
