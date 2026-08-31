"""Sembol detay: fiyat + katmanlar (SMA/Bollinger/VWAP/anomali), getiri, oynaklık,
işlem sayısı ve işlem büyüklüğü grafikleri."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import theme
from charts import PLOTLY_CFG, bars, explain, layout, line
from data import TZ, load_alerts


def render(df: pd.DataFrame, symbols: list[str], minutes: int, sma_s: int, sma_l: int) -> None:
    th = theme.current()
    sym = st.radio("Sembol", symbols, horizontal=True)
    g = df[df.symbol == sym]
    col = th.COLORS[sym]
    last = g.iloc[-1]

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Fiyat", f"${last.avg_price:,.2f}", f"{last.ret_pct:+.3f}% son dk")
    k2.metric("VWAP", f"${last.vwap:,.2f}", f"{(last.avg_price / last.vwap - 1) * 100:+.3f}% fiyat−VWAP")
    k3.metric("Oynaklık (σ, 30 dk)", f"{last.vol_pct:.3f}%" if pd.notna(last.vol_pct) else "—")
    k4.metric("İşlem / dk", f"{int(last.trade_count):,}",
              f"{last.z_trades:+.1f}σ" if pd.notna(last.z_trades) else None, delta_color="off")
    k5.metric("Ort. işlem", f"${last.avg_trade_usd:,.0f}")

    # --- fiyat + SMA/EMA + Bollinger + min-max bandı ---
    st.subheader(f"{sym} fiyat, hareketli ortalamalar ve Bollinger bandı")
    # Katman seçimi: grafik efsanesine (legend) tıklayarak kapatmak her 30 sn'lik
    # yenilemede sıfırlanıyordu (Plotly figürü baştan çiziliyor). Burada seçim
    # `key` ile Streamlit session_state'e yazılır → yenilemelerde korunur.
    # Varsayılan sade: sadece fiyat + anomali; diğer katmanları istersen aç.
    LAYERS = ["min-max bandı", "SMA", "Bollinger", "VWAP", "anomali", "Spark uyarıları"]
    layers = st.pills("Katmanlar", LAYERS, selection_mode="multi", default=["SMA", "anomali"],
                      key="detail_layers", help="Seçim otomatik yenilemede korunur") or []
    fig = go.Figure()
    if "min-max bandı" in layers:
        line(fig, g, "max_price", "dk içi en yüksek", th.MUTED, width=0.5, fmt=",.2f")
        line(fig, g, "min_price", "dk içi en düşük", th.MUTED, width=0.5, fmt=",.2f", fill="tonexty")
    if "Bollinger" in layers:
        line(fig, g, "bb_up", "Bollinger üst", th.MUTED, width=1, dash="dot")
        line(fig, g, "bb_lo", "Bollinger alt", th.MUTED, width=1, dash="dot")
    if "SMA" in layers:
        line(fig, g, "sma_l", f"SMA {sma_l}", th.WARN, width=1.5, dash="dash")
        line(fig, g, "sma_s", f"SMA {sma_s}", th.INK2, width=1.5)
    if "VWAP" in layers:
        line(fig, g, "vwap", "VWAP", th.POS, width=1.5, dash="dashdot")
    line(fig, g, "avg_price", "ortalama fiyat", col, width=2.5)
    # anomali işaretleri
    an = g[g.z_ret.abs() >= 2]
    if "anomali" in layers and not an.empty:
        fig.add_trace(go.Scatter(x=an["t"], y=an["avg_price"], mode="markers", name="anomali (|z|≥2)",
                                 marker=dict(color=th.WARN, size=10, symbol="diamond",
                                             line=dict(color=th.SURFACE, width=2)),
                                 hovertemplate="z=%{customdata:+.1f}σ", customdata=an["z_ret"]))
    # Spark'ın akış içinde ürettiği uyarılar (alerts tablosu) — tür başına farklı şekil
    if "Spark uyarıları" in layers:
        sa = load_alerts(minutes)
        sa = sa[sa.symbol == sym] if not sa.empty else sa
        if not sa.empty:
            sa = sa.merge(g[["window_start", "avg_price"]], on="window_start", how="inner", suffixes=("", "_g"))
            sa["t"] = sa["window_start"].dt.tz_convert(TZ)
            for kind, shape, label in (("ret_pct", "triangle-up", "Spark: fiyat"),
                                       ("trade_count", "square", "Spark: işlem"),
                                       ("volume_usd", "circle", "Spark: hacim")):
                k = sa[sa.kind == kind]
                if k.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=k["t"], y=k["avg_price_g"], mode="markers", name=label,
                    marker=dict(color=th.NEG, size=9, symbol=shape, line=dict(color=th.SURFACE, width=2)),
                    hovertemplate=label + " z=%{customdata:+.1f}σ", customdata=k["z"]))
    fig.update_layout(uirevision=sym)   # yenilemede zoom/legend durumunu koru (aynı sembolde)
    st.plotly_chart(layout(fig, "USD", height=420), width="stretch", config=PLOTLY_CFG, key="detail_price")
    explain("Fiyat grafiği katmanları", f"""
| Çizgi | Formül | Nasıl okunur |
|---|---|---|
| **Ortalama fiyat** (kalın, renkli) | Spark'ın hesapladığı `avg(price)` — o dakikadaki tüm işlemlerin ortalaması | Ana sinyal |
| **Gri bant** | `min(price)` – `max(price)` | Dakika içinde fiyat bu aralıkta gezmiş. Bant genişse dakika içi hareket sert |
| **SMA {sma_s}** (ince, açık) | son {sma_s} dakikanın basit ortalaması | Kısa vadeli eğilim; gürültüyü yumuşatır |
| **SMA {sma_l}** (kesikli, sarı) | son {sma_l} dakikanın ortalaması | Uzun vadeli eğilim. Kısa SMA uzunun **üstüne çıkarsa** yükseliş sinyali ("golden cross"), altına inerse düşüş |
| **VWAP** (yeşil noktalı-kesikli) | `Σ(fiyat×hacim) / Σ(hacim)` | Hacim ağırlıklı "adil fiyat". Fiyat VWAP'ın üstündeyse alıcılar baskın, altındaysa satıcılar |
| **Bollinger** (noktalı gri) | `SMA20 ± 2 × σ20` | Fiyat üst banda değerse istatistiksel olarak "pahalı", alt banda değerse "ucuz" bölge. Bant daralması genelde sert hareket öncesidir |
| **Sarı elmas** | dakikalık getirinin z-skoru `|z| ≥ 2` | Son 30 dk'ya göre olağan dışı sıçrama/düşüş |
""")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Dakikalık getiri (%)")
        fig = go.Figure(go.Bar(
            x=g["t"], y=g["ret_pct"], name="getiri %",
            marker=dict(color=np.where(g["ret_pct"] >= 0, th.POS, th.NEG), line=dict(width=0)),
            hovertemplate="%{y:+.3f}%"))
        fig.add_hline(y=0, line=dict(color=th.MUTED, width=1))
        st.plotly_chart(layout(fig, "%", legend=False), width="stretch", config=PLOTLY_CFG)
        explain("Dakikalık getiri", """
**Formül:** `ret_t = (fiyat_t / fiyat_(t−1) − 1) × 100`

Yeşil = o dakika yükselmiş, kırmızı = düşmüş. Çubukların **boyu** hareketin şiddeti.
Art arda aynı renk → momentum; sık renk değişimi → yatay/kararsız piyasa.
Oynaklık ve korelasyon hep bu seriden türetilir.
""")
    with c2:
        st.subheader("Oynaklık (hareketli σ, 30 dk) ve dakika içi salınım")
        fig = go.Figure()
        line(fig, g, "range_pct", "dk içi salınım % (max−min)/ort", th.MUTED, width=1, fmt=".3f")
        line(fig, g, "vol_pct", "oynaklık σ (30 dk)", col, width=2, fmt=".3f")
        st.plotly_chart(layout(fig, "%"), width="stretch", config=PLOTLY_CFG)
        explain("Oynaklık", """
- **σ (30 dk)**: son 30 dakikalık getirilerin standart sapması. Piyasanın "ne kadar sinirli"
  olduğunun ölçüsü; yükseliyorsa risk artıyor.
- **Dakika içi salınım**: `(max − min) / ortalama × 100`. Tek dakikanın içindeki gel-git.
  σ düşükken salınım aniden büyürse, bir sonraki dakikalarda oynaklık artışı beklenir.
""")

    c3, c4 = st.columns(2)
    with c3:
        st.subheader("İşlem sayısı / dk ve 30 dk ortalaması")
        fig = go.Figure()
        bars(fig, g, "trade_count", "işlem / dk", col)
        g_ma = g.assign(tc_ma=g.trade_count.rolling(30, min_periods=1).mean())
        line(fig, g_ma, "tc_ma", "30 dk ort.", th.WARN, width=1.5, dash="dash", fmt=",.0f")
        st.plotly_chart(layout(fig, "işlem"), width="stretch", config=PLOTLY_CFG)
        explain("İşlem sayısı", """
Spark'ın `count(*)`'ı: o dakika Binance'te kaç alım-satım eşleşmiş.
Sarı çizgi son 30 dk ortalaması. Çubuk ortalamanın **2 katına** çıkıyorsa
piyasaya ani ilgi var (haber, likidasyon dalgası). Anomaliler sekmesindeki `z_trades` bunu ölçer.
""")
    with c4:
        st.subheader("Ortalama işlem büyüklüğü ($)")
        fig = go.Figure()
        line(fig, g, "avg_trade_usd", "ort. işlem $", col, width=2, fmt="$,.0f", fill="tozeroy")
        st.plotly_chart(layout(fig, "USD", legend=False), width="stretch", config=PLOTLY_CFG)
        explain("İşlem büyüklüğü", """
**Formül:** `hacim_usd / işlem_sayısı`

Ortalama bir işlemin dolar büyüklüğü. Perakende yatırımcılar küçük, kurumsal/"balina"
işlemleri büyük olur. Fiyat düşerken bu değer yükseliyorsa büyük oyuncular satıyor;
fiyat yükselirken yükseliyorsa büyük alım var — aynı yüzde hareketi farklı anlam taşır.
""")
