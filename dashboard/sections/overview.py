"""Genel bakış: fiyat kartları, endeksli karşılaştırma, korelasyon, özet, hacim."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import theme
from charts import PLOTLY_CFG, bars, explain, layout, line


def render(df: pd.DataFrame, symbols: list[str]) -> None:
    th = theme.current()

    # --- kartlar ---
    cards = st.columns(len(symbols))
    for card, sym in zip(cards, symbols):
        g = df[df.symbol == sym]
        last = g.iloc[-1]
        card.metric(f"{sym} · {last.t.strftime('%H:%M')}", f"${last.avg_price:,.2f}",
                    f"{last.cum_ret_pct:+.2f}% (aralık başından beri)",
                    help=f"Son dk: {int(last.trade_count)} işlem · ${last.volume_usd:,.0f} hacim")

    # --- 100'e endeksli karşılaştırma ---
    st.subheader("Kim daha iyi gitti? — 100'e endeksli fiyat")
    fig = go.Figure()
    for sym in symbols:
        line(fig, df[df.symbol == sym], "index100", sym, th.COLORS[sym], fmt=".2f")
    fig.add_hline(y=100, line=dict(color=th.MUTED, width=1, dash="dot"))
    st.plotly_chart(layout(fig, "endeks (başlangıç = 100)"), width="stretch", config=PLOTLY_CFG)
    explain("Endeksli fiyat", """
**Formül:** `endeks_t = fiyat_t / fiyat_ilk × 100`

77.000 $'lık BTC ile 103 $'lık SOL'u aynı eksene koyamazsın; ikisini de seçili aralığın
ilk dakikasında 100'e eşitleyince **yüzde hareketleri** karşılaştırılabilir olur.
Çizgi 101 → %1 yükselmiş, 99 → %1 düşmüş. Çizgiler birlikte hareket ediyorsa piyasa
genel bir yöne gidiyor demektir; biri ayrışıyorsa o coin'e özel bir şey oluyor.
""")

    c1, c2 = st.columns([1, 1])
    # --- korelasyon ---
    with c1:
        st.subheader("Getiri korelasyonu")
        piv = df.pivot_table(index="window_start", columns="symbol", values="ret_pct")
        corr = piv.corr()
        fig = go.Figure(go.Heatmap(
            z=corr.values, x=corr.columns, y=corr.index, zmin=-1, zmax=1,
            xgap=3, ygap=3,   # hücreler arası yüzey boşluğu — ızgara okunur olsun
            colorscale=[[0, th.DIV_NEG], [0.5, th.DIV_MID], [1, th.DIV_POS]],
            text=np.round(corr.values, 2), texttemplate="%{text}", textfont=dict(color=th.INK),
            hovertemplate="%{x} × %{y}: %{z:.2f}<extra></extra>", showscale=False))
        st.plotly_chart(layout(fig, height=260, legend=False), width="stretch", config=PLOTLY_CFG)
        explain("Korelasyon", """
**Formül:** Pearson korelasyonu, dakikalık getiriler (`ret_pct`) üzerinden.

+1 → iki coin dakika dakika aynı yönde hareket ediyor; 0 → ilişkisiz; −1 → ters.
Kripto piyasasında BTC-ETH genelde 0.6–0.9 arası çıkar. Değer düşüyorsa
coin'ler birbirinden kopuyor (haber, coin'e özel olay). Renk: mavi = pozitif,
kırmızı = negatif, gri = sıfıra yakın (kutuplu veri için standart diverging skala).
""")

    # --- özet istatistik tablosu ---
    with c2:
        st.subheader("Aralık özeti")
        rows = []
        for sym in symbols:
            g = df[df.symbol == sym]
            rows.append({
                "Sembol": sym,
                "Son fiyat": g.avg_price.iloc[-1],
                "Getiri %": g.cum_ret_pct.iloc[-1],
                "En düşük": g.min_price.min(),
                "En yüksek": g.max_price.max(),
                "Oynaklık % (σ)": g.ret_pct.std(),
                "İşlem/dk (ort)": g.trade_count.mean(),
                "Hacim $ (toplam)": g.volume_usd.sum(),
                "Ort. işlem $": g.avg_trade_usd.mean(),
            })
        summ = pd.DataFrame(rows).set_index("Sembol")
        st.dataframe(summ.style.format({
            "Son fiyat": "${:,.2f}", "Getiri %": "{:+.2f}%", "En düşük": "${:,.2f}",
            "En yüksek": "${:,.2f}", "Oynaklık % (σ)": "{:.3f}%", "İşlem/dk (ort)": "{:,.0f}",
            "Hacim $ (toplam)": "${:,.0f}", "Ort. işlem $": "${:,.0f}"}), width="stretch")
        explain("Aralık özeti", """
- **Getiri %**: aralık başından sona fiyat değişimi.
- **Oynaklık (σ)**: dakikalık getirilerin standart sapması. %0.05 sakin, %0.3+ hareketli.
- **İşlem/dk**: Binance'te o coin'de dakikada kaç alım-satım olmuş (aktivite).
- **Hacim $**: `Σ (miktar × ort. fiyat)` — coin adedi yerine dolar, ki coin'ler karşılaştırılabilsin.
- **Ort. işlem $**: hacim / işlem sayısı. Yükseliyorsa büyük oyuncular ("balina") giriyor,
  düşüyorsa küçük perakende işlemleri hâkim.
""")

    # --- hacim payı ---
    st.subheader("Dolar hacmi / dk — hangi coin'de para dönüyor?")
    fig = go.Figure()
    for sym in symbols:
        bars(fig, df[df.symbol == sym], "volume_usd", sym, th.COLORS[sym], fmt="$,.0f")
    fig.update_layout(barmode="stack", bargap=0.25)
    st.plotly_chart(layout(fig, "USD"), width="stretch", config=PLOTLY_CFG)
    explain("Dolar hacmi", """
**Formül:** `hacim_usd = total_volume (coin adedi) × avg_price`

Yığılmış çubuk: toplam yükseklik = piyasada o dakika dönen para; renkler payı gösterir.
Ani yüksek çubuk = büyük bir hareket ya da haber anı; Anomaliler sekmesinde z-skoruyla işaretlenir.
""")
