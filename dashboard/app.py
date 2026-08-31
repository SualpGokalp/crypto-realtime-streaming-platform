r"""
Kripto akış analiz paneli — giriş noktası.

Kaynak: Spark'ın PostgreSQL'e yazdığı 1 dakikalık pencereler (price_windows).
Bu dosya yalnızca iskeleti kurar: sayfa ayarı, tema, üst şerit (filtreler),
veri yükleme ve bölüm yönlendirme. İçerik `sections/` altındaki modüllerde:

    theme.py                 renk paleti + CSS (açık/koyu tema)
    data.py                  PostgreSQL sorguları + gösterge hesapları (enrich)
    charts.py                ortak grafik yardımcıları (layout, line, bars, explain)
    sections/overview.py     Genel bakış
    sections/symbol_detail.py Sembol detay
    sections/deep_analysis.py Derin analiz
    sections/anomalies.py    Anomaliler & veri
    sections/ml_anomaly.py   ML Anomali (Isolation Forest)
    sections/health.py       Sağlık (boru hattı kontrolleri + log hataları)

Çalıştır:  .venv\Scripts\python -m streamlit run dashboard/app.py --server.port 8504
"""
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Crypto Streaming Panel", layout="wide")

import theme
from data import enrich, load
from sections import anomalies, deep_analysis, health, ml_anomaly, overview, symbol_detail

theme.inject_css(theme.current())

# ---------- üst şerit ----------
st.title("Crypto Streaming Panel")
st.caption("Binance → Kafka → Spark (1 dk pencere) → PostgreSQL → bu ekran · saatler TR (UTC+3)")

f1, f2, f3, f4 = st.columns([2, 2, 2, 1])
minutes = f1.select_slider("Zaman aralığı", [30, 60, 180, 360, 720, 1440], value=180,
                           format_func=lambda m: f"son {m} dk" if m < 60 else f"son {m // 60} sa")
sma_s = f2.slider("Kısa hareketli ort. (dk)", 3, 30, 5)
sma_l = f3.slider("Uzun hareketli ort. (dk)", 10, 120, 20)
refresh = f4.toggle("Oto-yenile", value=True, help="30 saniyede bir DB'den tazeler (Spark 30 sn'de bir yazar)")
if refresh:
    st_autorefresh(interval=30_000, key="tick")

raw = load(minutes)
if raw.empty:
    st.warning("Tabloda veri yok — aşağıdaki sağlık kontrolü hangi halkanın koptuğunu gösterir.")
    health.render(raw)
    st.stop()

symbols = sorted(raw["symbol"].unique())
df = enrich(raw, sma_s, sma_l, bb_n=20, z_n=30)

age = (pd.Timestamp.now(tz="UTC") - raw["updated_at"].max()).total_seconds()
if age > 90:
    st.error(f"Son yazma {age:.0f} sn önce — consumer durmuş olabilir.")
else:
    st.success(f"Canlı · son yazma {age:.0f} sn önce · {len(raw)} pencere satırı · {len(symbols)} sembol", icon="✅")

# st.tabs gizli sekmeleri de çizer (3 kat yük) → tek seferde yalnızca seçili bölümü çiz
SECTIONS = ["Genel bakış", "Sembol detay", "Derin analiz", "Anomaliler & veri", "ML Anomali", "Sağlık"]
# URL'den bölüm seçilebilsin: http://localhost:8504/?section=Derin%20analiz (paylaşılabilir link)
_qs = st.query_params.get("section", "Genel bakış")
section = st.segmented_control("Bölüm", SECTIONS, default=_qs if _qs in SECTIONS else "Genel bakış",
                               label_visibility="collapsed", key="section")
section = section or "Genel bakış"

if section == "Genel bakış":
    overview.render(df, symbols)
elif section == "Sembol detay":
    symbol_detail.render(df, symbols, minutes, sma_s, sma_l)
elif section == "Derin analiz":
    deep_analysis.render(df, symbols, minutes)
elif section == "Anomaliler & veri":
    anomalies.render(df, minutes)
elif section == "ML Anomali":
    ml_anomaly.render(df, symbols, minutes)
elif section == "Sağlık":
    health.render(raw)
