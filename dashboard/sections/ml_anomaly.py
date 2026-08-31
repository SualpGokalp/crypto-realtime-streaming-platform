"""ML Anomali: Isolation Forest ile çok değişkenli tespit, z-skorla karşılaştırma.

Düzeltilen hata: eski sürümde cache'li model fonksiyonu `df`'i dış kapsamdan
(closure) yakalıyordu — cache anahtarı df'i içermediği için veri değişse bile
30 sn boyunca bayat sonuç dönebilirdi. Artık df `_df` parametresiyle açıkça
geçirilir (baştaki alt çizgi: cache onu hash'lemesin, anahtar `cache_key`dir).
"""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import theme
from charts import PLOTLY_CFG, explain, layout, line

FEATURES = {
    "ret_pct":       "getiri % (dakikalık fiyat değişimi)",
    "range_pct":     "dk içi salınım % (max−min)/ort",
    "log_trades":    "log(işlem sayısı)",
    "log_volume":    "log($ hacim)",
    "avg_trade_usd": "ortalama işlem büyüklüğü $",
}


@st.cache_data(ttl=30)
def iforest_scores(_df: pd.DataFrame, cache_key: tuple, contamination: float) -> pd.DataFrame:
    """Her sembol için ayrı model: özellik ölçekleri ve 'normal'i coin'e özgüdür
    (BTC'de 4.000 işlem/dk sıradan, SOL'da anomali olurdu)."""
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import RobustScaler

    out = []
    for s, g in _df.groupby("symbol", sort=False):
        g = g.sort_values("window_start").copy()
        g["log_trades"] = np.log1p(g["trade_count"])
        g["log_volume"] = np.log1p(g["volume_usd"])
        feats = g[list(FEATURES)].replace([np.inf, -np.inf], np.nan)
        mask = feats.notna().all(axis=1)
        if mask.sum() < 30:                      # model için çok az örnek
            g["if_score"], g["if_flag"] = np.nan, False
            out.append(g)
            continue
        # RobustScaler: medyan/IQR ile ölçekler → aykırı değerler ölçeği bozamaz
        X = RobustScaler().fit_transform(feats[mask])
        model = IsolationForest(n_estimators=200, contamination=contamination,
                                random_state=42)
        model.fit(X)
        g.loc[mask, "if_score"] = -model.score_samples(X)   # büyük = daha anormal
        g.loc[mask, "if_flag"] = model.predict(X) == -1
        g["if_flag"] = g["if_flag"].fillna(False).astype(bool)
        out.append(g)
    return pd.concat(out)


def render(df: pd.DataFrame, symbols: list[str], minutes: int) -> None:
    th = theme.current()

    st.subheader("Isolation Forest — çok değişkenli anomali tespiti")
    st.caption("Z-skor her seriyi tek tek inceler; Isolation Forest 5 özelliğe **birlikte** bakar: "
               "tek tek normal görünen ama kombinasyonu tuhaf olan dakikaları da yakalar.")

    mc1, mc2 = st.columns([2, 3])
    cont = mc1.slider("Beklenen anomali oranı (contamination)", 0.5, 5.0, 2.0, 0.5,
                      format="%.1f%%",
                      help="Modelin 'verinin yüzde kaçı anomalidir' varsayımı. "
                           "%2 ≈ saatte ~1 dakika. Z-skor |z|≥2 eşiği de kabaca %4.6'ya denk gelir.") / 100
    sym_ml = mc2.radio("Sembol", symbols, horizontal=True, key="ml_symbol")

    # cache anahtarı: veri aralığı + satır sayısı (df'in kendisi hash'lenmesin diye)
    mldf = iforest_scores(df, (minutes, len(df), str(df.window_start.max())), cont)
    g = mldf[mldf.symbol == sym_ml]
    col = th.COLORS[sym_ml]
    flagged = g[g.if_flag]

    k1, k2, k3 = st.columns(3)
    k1.metric("İncelenen dakika", f"{g.if_score.notna().sum():,}")
    k2.metric("ML anomalisi", f"{len(flagged):,}")
    both = int((g.if_flag & (g.z_ret.abs() >= 2)).sum())
    k3.metric("Z-skor ile örtüşen", f"{both:,}",
              help="Hem Isolation Forest'ın hem |z(getiri)|≥2 kuralının işaretlediği dakikalar")

    # --- fiyat + ML işaretleri ---
    st.subheader(f"{sym_ml} fiyat ve ML anomalileri")
    fig = go.Figure()
    line(fig, g, "avg_price", "ortalama fiyat", col, width=2.5)
    # üç ayrı iz: yalnız z-skor, yalnız IF, her ikisi — çakışan dakikada işaretler
    # üst üste binip birbirini gizlemesin diye "her ikisi" kendi sembolünü alır
    zmask = g.z_ret.abs() >= 2
    zonly, ifonly, both_pts = g[zmask & ~g.if_flag], g[g.if_flag & ~zmask], g[zmask & g.if_flag]
    if not zonly.empty:
        fig.add_trace(go.Scatter(x=zonly["t"], y=zonly["avg_price"], mode="markers", name="yalnız z-skor (|z|≥2)",
                                 marker=dict(color=th.WARN, size=9, symbol="diamond",
                                             line=dict(color=th.SURFACE, width=1.5)),
                                 hovertemplate="z=%{customdata:+.1f}σ", customdata=zonly["z_ret"]))
    if not ifonly.empty:
        fig.add_trace(go.Scatter(x=ifonly["t"], y=ifonly["avg_price"], mode="markers",
                                 name="yalnız Isolation Forest",
                                 marker=dict(color=th.NEG, size=12, symbol="x-thin",
                                             line=dict(color=th.NEG, width=2.5)),
                                 hovertemplate="skor %{customdata:.3f}", customdata=ifonly["if_score"]))
    if not both_pts.empty:
        fig.add_trace(go.Scatter(x=both_pts["t"], y=both_pts["avg_price"], mode="markers",
                                 name="her ikisi (hemfikir)",
                                 marker=dict(color=th.POS, size=15, symbol="star",
                                             line=dict(color=th.SURFACE, width=1.5)),
                                 customdata=np.stack([both_pts["z_ret"], both_pts["if_score"]], axis=1),
                                 hovertemplate="z=%{customdata[0]:+.1f}σ · skor %{customdata[1]:.3f}"))
    fig.update_layout(uirevision=sym_ml)
    st.plotly_chart(layout(fig, "USD", height=380), width="stretch", config=PLOTLY_CFG)
    explain("İşaretler nasıl okunur?", """
- **Sarı elmas**: yalnız z-skor işaretlemiş (getiri, önceki 30 dk'ya göre sıra dışı).
- **Kırmızı çarpı**: yalnız Isolation Forest işaretlemiş (5 özelliğin kombinasyonu,
  seçili aralığın geneline göre sıra dışı).
- **Yeşil yıldız**: iki yöntem **hemfikir** → en güvenilir sinyal. Genelde büyük piyasa
  olaylarında görülür (ör. sert düşüş dakikası üç coin'de birden yıldızlanır).
- **Yalnız kırmızı çarpı** en ilginç durumdur: getiri tek başına normal ama örneğin
  hacim + işlem büyüklüğü + salınım birlikte tuhaf (balina girişi, dakika içi savaş,
  aşırı sessizlik). Z-skor bunları göremez.
- Yalnız sarı elmas: z-skora göre yerel bir sapma var ama aralığın geneline göre nadir
  değil — tipik örnek: çok sakin yarım saatin ortasındaki minik kıpırtı.
""")

    # --- skor zaman serisi ---
    st.subheader("Anomali skoru zaman serisi")
    thr_line = g.loc[g.if_flag, "if_score"].min() if not flagged.empty else None
    fig = go.Figure()
    line(fig, g, "if_score", "anomali skoru", col, width=2, fmt=".3f", fill="tozeroy")
    if thr_line is not None and pd.notna(thr_line):
        fig.add_hline(y=thr_line, line=dict(color=th.NEG, width=1, dash="dot"),
                      annotation_text=f"eşik ≈ {thr_line:.3f}", annotation_font_color=th.INK2)
    st.plotly_chart(layout(fig, "skor (yüksek = anormal)", height=260), width="stretch", config=PLOTLY_CFG)
    explain("Isolation Forest nasıl çalışır?", """
**Fikir:** 200 rastgele karar ağacı kurulur; her ağaç veriyi rastgele özellik + rastgele
eşiklerle böler. **Anormal noktalar azınlıkta ve uçta olduğu için birkaç bölmede tek
başına kalır** (izole olur); normal noktalar kalabalığın içinde olduğundan çok bölme gerekir.
Skor = "bu dakika ortalama kaç bölmede izole oldu"nun tersinden türetilir: erken izole
olan → yüksek skor → anomali.

**Neden z-skordan farklı/daha güçlü?**
- Z-skor **tek değişkenli**: her seriye ayrı bakar, "getiri normal AMA hacim+salınım+işlem
  büyüklüğü birlikte tuhaf" durumunu kaçırır. IF 5 boyutlu uzayda bakar.
- Z-skor normal dağılım varsayar; kripto getirileri kalın kuyrukludur. IF dağılım varsaymaz.
- Karşılığında IF'in eşiği daha az sezgiseldir: "σ cinsinden sapma" yerine soyut bir skor.
  Bu yüzden ikisi **birlikte** gösteriliyor — üretimde de tipik yaklaşım budur: basit kural
  + ML modeli yan yana, hemfikir olduklarında güven yüksek.

**Kurulum:** her sembole ayrı model (BTC'nin 'normal'i SOL'unkinden farklı),
`RobustScaler` ile ölçekleme (medyan/IQR — aykırı değerler ölçeği bozamaz),
`contamination` yukarıdaki kaydırıcıdan.
""")

    # --- işaretlenen dakikalar tablosu ---
    st.subheader("İşaretlenen dakikalar — model neye takıldı?")
    if flagged.empty:
        st.info("Seçili oran ve aralıkta ML anomalisi yok — contamination'ı artırmayı dene.")
    else:
        med = g[["ret_pct", "range_pct", "trade_count", "volume_usd", "avg_trade_usd"]].median()
        show = flagged.sort_values("if_score", ascending=False)[
            ["t", "if_score", "avg_price", "ret_pct", "range_pct", "trade_count", "volume_usd", "avg_trade_usd", "z_ret"]].copy()
        show["t"] = show["t"].dt.strftime("%d.%m %H:%M")
        st.dataframe(show.rename(columns={
            "t": "Zaman", "if_score": "Skor", "avg_price": "Fiyat", "ret_pct": "Getiri %",
            "range_pct": "Salınım %", "trade_count": "İşlem", "volume_usd": "Hacim $",
            "avg_trade_usd": "Ort. işlem $", "z_ret": "z(getiri)"}).style.format({
                "Skor": "{:.3f}", "Fiyat": "${:,.2f}", "Getiri %": "{:+.3f}%", "Salınım %": "{:.3f}%",
                "İşlem": "{:,.0f}", "Hacim $": "${:,.0f}", "Ort. işlem $": "${:,.0f}", "z(getiri)": "{:+.1f}"}),
            width="stretch", hide_index=True)
        st.caption(f"Aralık medyanları ({sym_ml}): getiri {med.ret_pct:+.3f}% · salınım {med.range_pct:.3f}% · "
                   f"{med.trade_count:,.0f} işlem/dk · ${med.volume_usd:,.0f} hacim · ort. işlem ${med.avg_trade_usd:,.0f} "
                   f"— tablodaki satırları bu 'normal'le kıyasla.")
    explain("Tablo nasıl okunur?", """
Satırlar skora göre sıralı: en üstteki, modelin en tuhaf bulduğu dakika. Hangi özelliğin
sorumlu olduğunu görmek için satırı alttaki **medyanlarla** karşılaştır: hacim medyanın
10 katıysa suçlu hacimdir; getiri ve hacim normalken ort. işlem $ çok yüksekse az sayıda
**büyük oyuncu** ("balina") girmiş demektir — z-skorun tek başına yakalayamayacağı örüntü.
`z(getiri)` sütunu küçükken skorun yüksek olması = modelin z-skora gerçek katkısı.
""")
