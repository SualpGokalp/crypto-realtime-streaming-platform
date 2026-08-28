r"""
Kripto akış analiz paneli.
Kaynak: Spark'ın PostgreSQL'e yazdığı 1 dakikalık pencereler (price_windows).
Bu dosya ham pencerelerden türetilmiş göstergeler (hareketli ortalama, getiri,
oynaklık, Bollinger, z-skor anomalileri, korelasyon) hesaplar ve her grafiğin
altında hesaplama açıklamasını gösterir.

Çalıştır:  .venv\Scripts\python -m streamlit run dashboard/app.py --server.port 8504
"""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

PG_DSN = "host=localhost port=5432 dbname=crypto user=crypto password=crypto"
TZ = "Europe/Istanbul"

st.set_page_config(page_title="Crypto Streaming Panel", layout="wide")

# ---------- tema & renkler (sabit sıra: coin → renk hiç değişmez) ----------
DARK = st.context.theme.type == "dark"
COLORS = ({"BTCUSDT": "#3987e5", "ETHUSDT": "#d95926", "SOLUSDT": "#199e70"} if DARK
          else {"BTCUSDT": "#2a78d6", "ETHUSDT": "#eb6834", "SOLUSDT": "#1baf7a"})
SURFACE = "#1a1a19" if DARK else "#fcfcfb"
GRID = "#383835" if DARK else "#e6e5e1"
INK = "#ffffff" if DARK else "#0b0b0b"
INK2 = "#c3c2b7" if DARK else "#52514e"
MUTED = "#6f6e69" if DARK else "#9a9892"      # yardımcı çizgiler (SMA, bant)
POS = "#0ca30c"                                # durum: iyi / pozitif
NEG = "#d03b3b"                                # durum: kritik / negatif
WARN = "#fab219"                               # durum: uyarı (anomali işareti)


# ---------- veri ----------
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


# ---------- grafik yardımcıları ----------
PLOTLY_CFG = {"displayModeBar": False, "scrollZoom": False}


def layout(fig: go.Figure, ytitle: str = "", height: int = 300, legend: bool = True) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=40), showlegend=legend,
        paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(color=INK, size=12), hovermode="x unified",
        legend=dict(orientation="h", y=1.04, x=0, font=dict(color=INK2)),
        xaxis=dict(showgrid=False, linecolor=GRID, tickfont=dict(color=INK2),
                   tickformat="%H:%M", hoverformat="%d.%m %H:%M"),
        yaxis=dict(title=ytitle, gridcolor=GRID, zeroline=False,
                   tickfont=dict(color=INK2), title_font=dict(color=INK2)),
    )
    return fig


def line(fig, g, col, name, color, width=2, dash=None, fmt=",.2f", fill=None):
    fig.add_trace(go.Scatter(
        x=g["t"], y=g[col], name=name, mode="lines",
        line=dict(color=color, width=width, dash=dash), fill=fill,
        fillcolor="rgba(128,128,128,0.10)" if fill else None,
        hovertemplate="%{y:" + fmt + "}",
    ))


def bars(fig, g, col, name, color, fmt=","):
    fig.add_trace(go.Bar(x=g["t"], y=g[col], name=name,
                         marker=dict(color=color, line=dict(width=0)),
                         hovertemplate="%{y:" + fmt + "}"))


def explain(title: str, body: str):
    """Grafiğin altındaki 'nasıl hesaplandı' kutusu."""
    with st.expander(f"ⓘ {title} — nasıl hesaplanıyor, nasıl okunur?"):
        st.markdown(body)


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
    st.warning("Tabloda veri yok. Producer + consumer çalışıyor mu? (docker ps, terminaller)")
    st.stop()

symbols = sorted(raw["symbol"].unique())
df = enrich(raw, sma_s, sma_l, bb_n=20, z_n=30)

age = (pd.Timestamp.now(tz="UTC") - raw["updated_at"].max()).total_seconds()
if age > 90:
    st.error(f"Son yazma {age:.0f} sn önce — consumer durmuş olabilir.")
else:
    st.success(f"Canlı · son yazma {age:.0f} sn önce · {len(raw)} pencere satırı · {len(symbols)} sembol", icon="✅")

# st.tabs gizli sekmeleri de çizer (3 kat yük) → tek seferde yalnızca seçili bölümü çiz
section = st.segmented_control("Bölüm", ["Genel bakış", "Sembol detay", "Anomaliler & veri"],
                               default="Genel bakış", label_visibility="collapsed")
section = section or "Genel bakış"

# =====================================================================
# 1) GENEL BAKIŞ
# =====================================================================
if section == "Genel bakış":
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
        line(fig, df[df.symbol == sym], "index100", sym, COLORS[sym], fmt=".2f")
    fig.add_hline(y=100, line=dict(color=MUTED, width=1, dash="dot"))
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
            colorscale=[[0, "#d95926"], [0.5, "#383835" if DARK else "#f0efec"], [1, "#2a78d6"]],
            text=np.round(corr.values, 2), texttemplate="%{text}", textfont=dict(color=INK),
            hovertemplate="%{x} × %{y}: %{z:.2f}<extra></extra>", showscale=False))
        st.plotly_chart(layout(fig, height=260, legend=False), width="stretch", config=PLOTLY_CFG)
        explain("Korelasyon", """
**Formül:** Pearson korelasyonu, dakikalık getiriler (`ret_pct`) üzerinden.

+1 → iki coin dakika dakika aynı yönde hareket ediyor; 0 → ilişkisiz; −1 → ters.
Kripto piyasasında BTC-ETH genelde 0.6–0.9 arası çıkar. Değer düşüyorsa
coin'ler birbirinden kopuyor (haber, coin'e özel olay). Renk: mavi = pozitif,
turuncu = negatif, gri = sıfıra yakın.
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
            "Hacim $ (toplam)": "${:,.0f}", "Ort. işlem $": "${:,.0f}"}), width="stretch", config=PLOTLY_CFG)
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
        bars(fig, df[df.symbol == sym], "volume_usd", sym, COLORS[sym], fmt="$,.0f")
    fig.update_layout(barmode="stack", bargap=0.25)
    st.plotly_chart(layout(fig, "USD"), width="stretch", config=PLOTLY_CFG)
    explain("Dolar hacmi", """
**Formül:** `hacim_usd = total_volume (coin adedi) × avg_price`

Yığılmış çubuk: toplam yükseklik = piyasada o dakika dönen para; renkler payı gösterir.
Ani yüksek çubuk = büyük bir hareket ya da haber anı; Anomaliler sekmesinde z-skoruyla işaretlenir.
""")

# =====================================================================
# 2) SEMBOL DETAY
# =====================================================================
if section == "Sembol detay":
    sym = st.radio("Sembol", symbols, horizontal=True)
    g = df[df.symbol == sym]
    col = COLORS[sym]
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
    fig = go.Figure()
    line(fig, g, "max_price", "dk içi en yüksek", MUTED, width=0.5, fmt=",.2f")
    line(fig, g, "min_price", "dk içi en düşük", MUTED, width=0.5, fmt=",.2f", fill="tonexty")
    line(fig, g, "bb_up", "Bollinger üst", MUTED, width=1, dash="dot")
    line(fig, g, "bb_lo", "Bollinger alt", MUTED, width=1, dash="dot")
    line(fig, g, "sma_l", f"SMA {sma_l}", WARN, width=1.5, dash="dash")
    line(fig, g, "sma_s", f"SMA {sma_s}", INK2, width=1.5)
    line(fig, g, "vwap", "VWAP", POS, width=1.5, dash="dashdot")
    line(fig, g, "avg_price", "ortalama fiyat", col, width=2.5)
    # anomali işaretleri
    an = g[g.z_ret.abs() >= 2]
    if not an.empty:
        fig.add_trace(go.Scatter(x=an["t"], y=an["avg_price"], mode="markers", name="anomali (|z|≥2)",
                                 marker=dict(color=WARN, size=10, symbol="diamond",
                                             line=dict(color=SURFACE, width=2)),
                                 hovertemplate="z=%{customdata:+.1f}σ", customdata=an["z_ret"]))
    st.plotly_chart(layout(fig, "USD", height=420), width="stretch", config=PLOTLY_CFG)
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
            marker=dict(color=np.where(g["ret_pct"] >= 0, POS, NEG), line=dict(width=0)),
            hovertemplate="%{y:+.3f}%"))
        fig.add_hline(y=0, line=dict(color=MUTED, width=1))
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
        line(fig, g, "range_pct", "dk içi salınım % (max−min)/ort", MUTED, width=1, fmt=".3f")
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
        line(fig, g_ma, "tc_ma", "30 dk ort.", WARN, width=1.5, dash="dash", fmt=",.0f")
        st.plotly_chart(layout(fig, "işlem"), width="stretch", config=PLOTLY_CFG)
        explain("İşlem sayısı", """
Spark'ın `count(*)`'ı: o dakika Binance'te kaç alım-satım eşleşmiş.
Sarı çizgi son 30 dk ortalaması. Çubuk ortalamanın **2 katına** çıkıyorsa
piyasaya ani ilgi var (haber, likidasyon dalgası). Anomaliler sekmesindeki `z_trades` bunu ölçer.
""")
    with c4:
        st.subheader("Ortalama işlem büyüklüğü ($)")
        fig = go.Figure()
        line(fig, g, "avg_trade_usd", "ort. işlem $", col, width=2, fmt="$,.0f")
        st.plotly_chart(layout(fig, "USD", legend=False), width="stretch", config=PLOTLY_CFG)
        explain("İşlem büyüklüğü", """
**Formül:** `hacim_usd / işlem_sayısı`

Ortalama bir işlemin dolar büyüklüğü. Perakende yatırımcılar küçük, kurumsal/"balina"
işlemleri büyük olur. Fiyat düşerken bu değer yükseliyorsa büyük oyuncular satıyor;
fiyat yükselirken yükseliyorsa büyük alım var — aynı yüzde hareketi farklı anlam taşır.
""")

# =====================================================================
# 3) ANOMALİLER & VERİ
# =====================================================================
if section == "Anomaliler & veri":
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
Bu, akış projelerinde en basit ve en yaygın **gerçek zamanlı uyarı** yöntemidir; bir sonraki
adımda aynı hesabı Spark tarafına taşıyıp Kafka'ya "alert" topic'i yazdırmak mümkün.
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
