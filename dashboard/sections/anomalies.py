"""Anomaliler & veri: panel içi z-skor tablosu, Spark uyarıları, ham veri + CSV."""
import numpy as np
import pandas as pd
import streamlit as st

from charts import explain
from data import TZ, load_alerts


def render(df: pd.DataFrame, minutes: int) -> None:
    st.subheader("Olağan dışı dakikalar")
    thr = st.slider("Eşik (|z| ≥)", 1.5, 4.0, 2.0, 0.5)
    an = df[(df.z_ret.abs() >= thr) | (df.z_trades.abs() >= thr) | (df.z_vol.abs() >= thr)].copy()
    an["neden"] = np.select(
        [an.z_ret.abs() >= thr, an.z_vol.abs() >= thr, an.z_trades.abs() >= thr],
        ["fiyat sıçraması", "hacim patlaması", "işlem sayısı patlaması"], "—")
    if an.empty:
        st.info("Seçili eşikte anomali yok — piyasa sakin.")
    else:
        show = an.sort_values("window_start", ascending=False)[
            ["t", "symbol", "neden", "avg_price", "ret_pct", "z_ret", "trade_count", "z_trades", "volume_usd", "z_vol"]]
        show["t"] = show["t"].dt.strftime("%d.%m %H:%M")
        st.dataframe(show.rename(columns={
            "t": "Zaman", "symbol": "Sembol", "avg_price": "Fiyat", "ret_pct": "Getiri %",
            "z_ret": "z(getiri)", "trade_count": "İşlem", "z_trades": "z(işlem)",
            "volume_usd": "Hacim $", "z_vol": "z(hacim)"}).style.format({
                "Fiyat": "${:,.2f}", "Getiri %": "{:+.3f}%", "z(getiri)": "{:+.1f}",
                "İşlem": "{:,.0f}", "z(işlem)": "{:+.1f}", "Hacim $": "${:,.0f}", "z(hacim)": "{:+.1f}"}),
            width="stretch", hide_index=True)
    explain("Z-skor anomali tespiti", """
**Formül:** `z_t = (x_t − ort(x, önceki 30 dk)) / σ(x, önceki 30 dk)`

Üç seri için ayrı ayrı hesaplanır: dakikalık getiri, işlem sayısı, dolar hacmi.
Ortalama ve σ **o dakikayı içermeden** (bir önceki 30 dk) alınır; yoksa aykırı değer kendi
eşiğini şişirir. |z| ≥ 2 → "normalde 20 dakikada bir görülür" seviyesi, |z| ≥ 3 → nadir.
Yukarıdaki tablo bu hesabı **panelde, seçili aralık üzerinde** yapar (eşik kaydırılabilir).
Aynı hesap artık **akış içinde de** çalışıyor — aşağıdaki tablo Spark'ın ürettiği uyarılardır.
""")

    st.subheader("Spark uyarıları (akış içinde tespit)")
    al = load_alerts(minutes)
    if al.empty:
        st.info("Bu aralıkta Spark uyarısı yok. (Consumer'ın anomali sorgusu açık mı? Eşik: ALERT_Z, varsayılan 2.0)")
    else:
        al["Zaman"] = al["window_start"].dt.tz_convert(TZ).dt.strftime("%d.%m %H:%M")
        al["Gecikme (sn)"] = (al["detected_at"] - al["window_start"]).dt.total_seconds() - 60  # pencere bitiminden itibaren
        al["Tür"] = al["kind"].map({"ret_pct": "fiyat sıçraması", "volume_usd": "hacim patlaması",
                                    "trade_count": "işlem sayısı patlaması"})
        st.dataframe(al[["Zaman", "symbol", "Tür", "value", "baseline", "sigma", "z", "avg_price", "Gecikme (sn)"]]
                     .rename(columns={"symbol": "Sembol", "value": "Değer", "baseline": "Ort (30 dk)",
                                      "sigma": "σ", "z": "z", "avg_price": "Fiyat"})
                     .style.format({"Değer": "{:,.3f}", "Ort (30 dk)": "{:,.3f}", "σ": "{:,.3f}",
                                    "z": "{:+.1f}", "Fiyat": "${:,.2f}", "Gecikme (sn)": "{:.0f}"}),
                     width="stretch", hide_index=True)
    explain("Akış içi uyarı nasıl üretiliyor?", """
`consumer/spark_consumer.py` aynı 1 dk pencereyi **ikinci bir sorguyla `append` modunda** dinler:
pencere ancak watermark geçince (kesinleşince) gelir, yarım pencereye sahte alarm üretilmez.
Kesinleşen pencere için `consumer/alerts.py` önceki 30 dakikayı `price_windows` tablosundan
çeker, üç seri için z-skoru hesaplar ve eşiği aşanları **Kafka `crypto-alerts` topic'ine**
(JSON, key=sembol) ve **`alerts` tablosuna** yazar. "Gecikme" = pencere bitiminden tespite
geçen süre; watermark (30 sn) + trigger (30 sn) nedeniyle tipik olarak 30-90 sn.
Panel tablosuyla küçük farklar normaldir: panel seçili aralığa göre pandas ile hesaplar,
Spark ise her dakikayı geldiği anda tek başına değerlendirir.
""")

    st.subheader("Ham + türetilmiş veri")
    cols = ["t", "symbol", "avg_price", "min_price", "max_price", "trade_count", "total_volume",
            "volume_usd", "avg_trade_usd", "ret_pct", "range_pct", "sma_s", "sma_l", "vwap",
            "bb_up", "bb_lo", "vol_pct", "z_ret", "z_trades", "z_vol"]
    tbl = df.sort_values(["window_start", "symbol"], ascending=[False, True])[cols].copy()
    tbl["t"] = tbl["t"].dt.strftime("%d.%m %H:%M")
    st.dataframe(tbl, width="stretch", hide_index=True, height=360)
    st.download_button("CSV indir", tbl.to_csv(index=False).encode("utf-8"),
                       file_name="price_windows_enriched.csv", mime="text/csv")
    explain("Sütun sözlüğü", """
`avg/min/max_price, trade_count, total_volume` → Spark'tan gelen ham pencere.
`volume_usd` hacim×fiyat · `avg_trade_usd` hacim/işlem · `ret_pct` dakikalık getiri ·
`range_pct` dakika içi salınım · `sma_s/sma_l` hareketli ortalamalar · `vwap` hacim ağırlıklı
ortalama · `bb_up/bb_lo` Bollinger · `vol_pct` 30 dk σ · `z_*` anomali skorları.
""")
