"""Derin analiz: drawdown, hareketli korelasyon, getiri dağılımı, hacim-getiri
scatter'ı, Spark uyarı zaman çizelgesi, saatlik aktivite profili."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import theme
from charts import PLOTLY_CFG, explain, layout, line
from data import TZ, load_alerts, load_profile

KIND_TR = {"ret_pct": "fiyat", "trade_count": "işlem", "volume_usd": "hacim"}


def render(df: pd.DataFrame, symbols: list[str], minutes: int) -> None:
    th = theme.current()
    al = load_alerts(minutes)
    if not al.empty:
        al["t"] = al["window_start"].dt.tz_convert(TZ)

    # --- 1) Zirveden düşüş (drawdown) ---
    st.subheader("Zirveden düşüş — aralık içindeki en yüksek fiyata göre % kayıp")
    fig = go.Figure()
    for sym in symbols:
        gg = df[df.symbol == sym].copy()
        gg["dd"] = (gg.avg_price / gg.avg_price.cummax() - 1) * 100
        line(fig, gg, "dd", sym, th.COLORS[sym], fmt="+.2f", fill="tozeroy")
    fig.add_hline(y=0, line=dict(color=th.MUTED, width=1))
    st.plotly_chart(layout(fig, "%", height=260), width="stretch", config=PLOTLY_CFG)
    explain("Zirveden düşüş (drawdown)", """
**Formül:** `dd_t = fiyat_t / max(fiyat_0..t) − 1` (×100)

Çizgi 0'daysa coin aralık içindeki **zirvesinde**; −1.5 ise zirveden %1.5 aşağıda.
Fiyat grafiğinde "düştü mü" görürsün, burada **ne kadar** ve **ne süre** düşük kaldığını
görürsün. Uzun süre 0'a dönememek zayıflık; sık sık 0'a dokunmak güçlü trend demektir.
Üç coin'i aynı eksende karşılaştırmak için yüzde kullanılır.
""")

    c1, c2 = st.columns(2)
    # --- 2) Hareketli korelasyon ---
    with c1:
        st.subheader("Hareketli korelasyon (30 dk) — coin'ler ne zaman ayrışıyor?")
        piv = df.pivot_table(index="window_start", columns="symbol", values="ret_pct").sort_index()
        pairs = [(a, b) for i, a in enumerate(symbols) for b in symbols[i + 1:] if a in piv and b in piv]
        fig = go.Figure()
        # çift rengi: çiftin BTC olmayan üyesi (BTC-ETH → ETH rengi, BTC-SOL → SOL, ETH-SOL → gri)
        rc_all = []
        for a, b in pairs:
            rc = piv[a].rolling(30, min_periods=10).corr(piv[b])
            rc_all.append(rc)
            rcdf = pd.DataFrame({"t": piv.index.tz_convert(TZ), "rc": rc.values})
            color = th.COLORS[b] if a == "BTCUSDT" else th.MUTED
            line(fig, rcdf, "rc", f"{a[:3]}–{b[:3]}", color, fmt=".2f")
        # Eksen veriye göre daralır: korelasyon çoğu zaman 0.7-1.0 bandında gezer,
        # sabit -1..+1 ekseninde çizgiler tavana yapışıp okunmaz oluyordu.
        lo = float(np.nanmin(pd.concat(rc_all))) if rc_all else -1.0
        ylo = max(-1.05, lo - 0.08)
        if ylo <= 0:
            fig.add_hline(y=0, line=dict(color=th.MUTED, width=1))
        fig.update_yaxes(range=[ylo, 1.03])
        st.plotly_chart(layout(fig, "korelasyon", height=280), width="stretch", config=PLOTLY_CFG)
        explain("Hareketli korelasyon", """
**Formül:** her dakika için, son 30 dakikanın getirileri üzerinden Pearson korelasyonu.

Genel bakıştaki ısı haritası tüm aralığın **tek** sayısını verir; burada bu sayının
**zamanla nasıl değiştiğini** görürsün. Normalde 0.5–0.9 arasında gezer.
Çizgi aniden 0'a ya da eksiye düşerse coin'ler ayrışmıştır: birine özel bir haber/olay
var demektir — hangisi olduğunu zirveden düşüş grafiğinde bulabilirsin.
""")

    # --- 3) Getiri dağılımı (small multiples) ---
    with c2:
        st.subheader("Getiri dağılımı — 'normal' dakika nasıl görünür?")
        rows = st.columns(len(symbols))
        for colm, sym in zip(rows, symbols):
            r = df[df.symbol == sym].ret_pct.dropna()
            if len(r) < 10:
                colm.info(f"{sym}: veri az")
                continue
            sd = r.std()
            share = (r.abs() >= 2 * sd).mean() * 100
            fig = go.Figure(go.Histogram(x=r, nbinsx=40,
                                         marker=dict(color=th.COLORS[sym], line=dict(width=0)),
                                         hovertemplate="%{x:.3f}% · %{y} dk<extra></extra>", name=sym))
            for k in (-2, 2):
                fig.add_vline(x=k * sd, line=dict(color=th.WARN, width=1.5, dash="dot"))
            fig.update_layout(bargap=0.05, showlegend=False)
            colm.plotly_chart(layout(fig, "dakika", height=220, legend=False)
                              .update_xaxes(title=f"{sym[:3]} · |z|≥2: %{share:.1f}", tickformat=".2f"),
                              width="stretch", config=PLOTLY_CFG)
        explain("Getiri dağılımı", """
Her çubuk: dakikalık getirinin o değerde kaç kez görüldüğü. Sarı noktalı çizgiler
**±2σ** — anomali eşiğimiz. Çan eğrisi gibi görünür; uçlarda kalan az sayıda dakika
"anomali" dediklerimizdir. Eksen altındaki yüzde, aralıkta kaç dakikanın eşiği aştığı;
tam normal dağılımda ~%4.6 olur. Daha yüksekse piyasa "kalın kuyruklu" (sert hareket
sık), daha düşükse sakin. Dağılım sağa/sola kaymışsa aralık boyunca yön eğilimi var.
""")

    # --- 4) Hareket–hacim ilişkisi ---
    st.subheader("Hareket ne kadar hacimle desteklendi? — dakikalık getiri vs $ hacim")
    # Coin seçimi grafiğin efsanesinden değil buradan: efsane tıklamaları 30 sn'lik
    # oto-yenilemede sıfırlanıyordu; `key`'li pills seçimi session_state'te kalıcıdır.
    sc_syms = st.pills("Gösterilen coinler", symbols, selection_mode="multi",
                       default=symbols, key="scatter_syms",
                       help="Seçim otomatik yenilemede korunur") or symbols
    fig = go.Figure()
    for sym in sc_syms:
        gg = df[df.symbol == sym].dropna(subset=["ret_pct", "volume_usd"])
        fig.add_trace(go.Scatter(
            x=gg["volume_usd"], y=gg["ret_pct"], mode="markers", name=sym,
            marker=dict(color=th.COLORS[sym], size=7, opacity=0.55, line=dict(width=0)),
            customdata=gg["t"].dt.strftime("%d.%m %H:%M"),
            hovertemplate="%{customdata}<br>hacim $%{x:,.0f}<br>getiri %{y:+.3f}%<extra>" + sym + "</extra>"))
    if not al.empty:
        # uyarı halkaları da seçime uyar: yalnız gösterilen coin'lerin uyarıları çizilir
        ap = al[(al.kind == "ret_pct") & (al.symbol.isin(sc_syms))].merge(
            df[["window_start", "symbol", "ret_pct", "volume_usd"]],
            on=["window_start", "symbol"], how="inner")
        if not ap.empty:
            fig.add_trace(go.Scatter(
                x=ap["volume_usd"], y=ap["ret_pct"], mode="markers", name="Spark fiyat uyarısı",
                marker=dict(color="rgba(0,0,0,0)", size=13, line=dict(color=th.NEG, width=2)),
                hovertemplate="z=%{customdata:+.1f}σ<extra>uyarı</extra>", customdata=ap["z"]))
    fig.update_xaxes(type="log", title="$ hacim / dk (log)",
                     tickvals=[1e3, 1e4, 1e5, 1e6, 1e7], ticktext=["$1K", "$10K", "$100K", "$1M", "$10M"])
    fig.add_hline(y=0, line=dict(color=th.MUTED, width=1))
    st.plotly_chart(layout(fig, "getiri %", height=340), width="stretch", config=PLOTLY_CFG)
    explain("Hareket–hacim", """
Her nokta bir dakika: sağa gittikçe hacim büyük, yukarı/aşağı gittikçe fiyat hareketi büyük.
Hacim ekseni logaritmik (10×'lar eşit aralık), çünkü hacim dakikadan dakikaya 100 kat değişebilir.

- **Sağ üst / sağ alt**: büyük hareket + büyük hacim → gerçek, "desteklenmiş" hareket.
- **Sol üst / sol alt**: büyük hareket ama küçük hacim → sığ piyasa, kolay geri dönebilir.
- **Kırmızı halka**: Spark'ın fiyat uyarısı verdiği dakika. Halkalar sağda toplanıyorsa
  anomaliler hacimle geliyor (haber), solda ise gürültü.
""")

    # --- 5) Uyarı zaman çizelgesi ---
    st.subheader("Spark uyarı zaman çizelgesi — kim, ne zaman, ne kadar sıra dışı?")
    if al.empty:
        st.info("Bu aralıkta Spark uyarısı yok.")
    else:
        fig = go.Figure()
        shapes = {"ret_pct": "triangle-up", "trade_count": "square", "volume_usd": "circle"}
        for sym in symbols:
            for kind, shape in shapes.items():
                k = al[(al.symbol == sym) & (al.kind == kind)]
                if k.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=k["t"], y=[sym] * len(k), mode="markers", name=f"{sym[:3]} · {KIND_TR[kind]}",
                    legendgroup=sym,
                    marker=dict(color=th.COLORS[sym], symbol=shape,
                                line=dict(color=th.SURFACE, width=1.5),
                                size=(6 + 2.5 * k["z"].abs().clip(upper=8)).tolist()),
                    customdata=np.stack([k["z"], k["value"], k["baseline"]], axis=1),
                    hovertemplate=("%{x|%d.%m %H:%M} · " + KIND_TR[kind] +
                                   "<br>z=%{customdata[0]:+.1f}σ · değer %{customdata[1]:,.3f} · ort %{customdata[2]:,.3f}<extra></extra>")))
        fig.update_yaxes(categoryorder="array", categoryarray=symbols[::-1])
        st.plotly_chart(layout(fig, "", height=260), width="stretch", config=PLOTLY_CFG)
        explain("Uyarı zaman çizelgesi", """
Her işaret Spark'ın akış içinde ürettiği bir uyarı. **Şekil** = tür (▲ fiyat, ■ işlem sayısı,
● hacim), **büyüklük** = |z| (ne kadar sıra dışı), **renk** = coin.
Aynı dakikada üç coin'de birden işaret varsa olay piyasa geneli (makro haber);
tek coin'de kümelenmişse o coin'e özel. Bir dakikada ▲+■+● üçü birden → en güçlü sinyal:
fiyat hacim ve işlem sayısıyla birlikte hareket etmiş.
""")

    # --- 6) Günün saatine göre aktivite ---
    st.subheader("Günün saatine göre aktivite — piyasa ne zaman uyanıyor? (son 7 gün, TR saati)")
    prof = load_profile()
    hours_covered = prof.groupby("symbol").hour.nunique().min() if not prof.empty else 0
    if prof.empty or hours_covered < 3:
        st.info("Profil için en az birkaç saatlik veri gerekir; consumer çalıştıkça dolar.")
    else:
        fig = go.Figure()
        for sym in symbols:
            pp = prof[prof.symbol == sym].copy()
            # BTC'de dakikada 1.500, SOL'da 300 işlem olur; aynı eksende SOL okunmaz.
            # Her coin kendi ortalamasına endekslenir: 100 = o coin'in ortalama saati.
            pp["idx"] = pp["trades"] / pp["trades"].mean() * 100
            fig.add_trace(go.Bar(x=pp["hour"], y=pp["idx"], name=sym,
                                 marker=dict(color=th.COLORS[sym], line=dict(width=0)),
                                 customdata=np.stack([pp["trades"], pp["n"]], axis=1),
                                 hovertemplate="%{x}:00 · endeks %{y:.0f} · %{customdata[0]:,.0f} işlem/dk · %{customdata[1]} dk örnek<extra>" + sym + "</extra>"))
        fig.add_hline(y=100, line=dict(color=th.MUTED, width=1, dash="dot"))
        fig.update_layout(barmode="group", bargap=0.25)
        fig.update_xaxes(dtick=1, title="saat (TR)")
        st.plotly_chart(layout(fig, "aktivite endeksi (100 = coin'in ortalaması)", height=280),
                        width="stretch", config=PLOTLY_CFG)
        if hours_covered < 24:
            st.caption(f"Henüz günün {hours_covered} saati örneklendi; 24 saat dolunca profil tamamlanır.")
        explain("Günün saatine göre aktivite", """
Son 7 günün tüm dakikaları, günün saatine göre gruplanıp ortalaması alınır; sonra her coin
kendi ortalamasına bölünür (100 = o coin için sıradan bir saat, 150 = ortalamanın 1.5 katı).
Böylece BTC'nin 1.500 işlem/dk'sı ile SOL'un 300'ü aynı eksende karşılaştırılabilir.
Kripto 7/24 açık ama insanlar değil: genelde **16:00–23:00 TR** (ABD seansı) en hareketli,
sabaha karşı en sakin saatlerdir. Bunu bilmek anomali yorumunu değiştirir: gece 04:00'te
500 işlem/dk sıra dışıdır, akşam 20:00'de normaldir. (Anomali hesabı zaten son 30 dk'ya
göre olduğu için buna kısmen uyum sağlar; bu grafik "büyük resmi" verir.)
""")
