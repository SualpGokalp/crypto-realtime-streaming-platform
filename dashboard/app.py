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


# ---------- grafik yardımcıları ----------
PLOTLY_CFG = {"displayModeBar": False, "scrollZoom": False}


def layout(fig: go.Figure, ytitle: str = "", height: int = 300, legend: bool = True) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=40), showlegend=legend,
        uirevision="keep",   # yenilemede kullanıcının zoom/legend seçimlerini koru

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
SECTIONS = ["Genel bakış", "Sembol detay", "Derin analiz", "Anomaliler & veri", "ML Anomali"]
# URL'den bölüm seçilebilsin: http://localhost:8504/?section=Derin%20analiz (paylaşılabilir link)
_qs = st.query_params.get("section", "Genel bakış")
section = st.segmented_control("Bölüm", SECTIONS, default=_qs if _qs in SECTIONS else "Genel bakış",
                               label_visibility="collapsed", key="section")
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
    # Katman seçimi: grafik efsanesine (legend) tıklayarak kapatmak her 30 sn'lik
    # yenilemede sıfırlanıyordu (Plotly figürü baştan çiziliyor). Burada seçim
    # `key` ile Streamlit session_state'e yazılır → yenilemelerde korunur.
    # Varsayılan sade: sadece fiyat + anomali; diğer katmanları istersen aç.
    LAYERS = ["min-max bandı", "SMA", "Bollinger", "VWAP", "anomali", "Spark uyarıları"]
    layers = st.pills("Katmanlar", LAYERS, selection_mode="multi", default=["SMA", "anomali"],
                      key="detail_layers", help="Seçim otomatik yenilemede korunur") or []
    fig = go.Figure()
    if "min-max bandı" in layers:
        line(fig, g, "max_price", "dk içi en yüksek", MUTED, width=0.5, fmt=",.2f")
        line(fig, g, "min_price", "dk içi en düşük", MUTED, width=0.5, fmt=",.2f", fill="tonexty")
    if "Bollinger" in layers:
        line(fig, g, "bb_up", "Bollinger üst", MUTED, width=1, dash="dot")
        line(fig, g, "bb_lo", "Bollinger alt", MUTED, width=1, dash="dot")
    if "SMA" in layers:
        line(fig, g, "sma_l", f"SMA {sma_l}", WARN, width=1.5, dash="dash")
        line(fig, g, "sma_s", f"SMA {sma_s}", INK2, width=1.5)
    if "VWAP" in layers:
        line(fig, g, "vwap", "VWAP", POS, width=1.5, dash="dashdot")
    line(fig, g, "avg_price", "ortalama fiyat", col, width=2.5)
    # anomali işaretleri
    an = g[g.z_ret.abs() >= 2]
    if "anomali" in layers and not an.empty:
        fig.add_trace(go.Scatter(x=an["t"], y=an["avg_price"], mode="markers", name="anomali (|z|≥2)",
                                 marker=dict(color=WARN, size=10, symbol="diamond",
                                             line=dict(color=SURFACE, width=2)),
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
                    marker=dict(color=NEG, size=9, symbol=shape, line=dict(color=SURFACE, width=2)),
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

# =====================================================================
# 4) DERİN ANALİZ — "ne oldu"dan "neden / ne kadar sıra dışı"ya
# =====================================================================
if section == "Derin analiz":
    KIND_TR = {"ret_pct": "fiyat", "trade_count": "işlem", "volume_usd": "hacim"}
    al = load_alerts(minutes)
    if not al.empty:
        al["t"] = al["window_start"].dt.tz_convert(TZ)

    # --- 1) Zirveden düşüş (drawdown) ---
    st.subheader("Zirveden düşüş — aralık içindeki en yüksek fiyata göre % kayıp")
    fig = go.Figure()
    for sym in symbols:
        gg = df[df.symbol == sym].copy()
        gg["dd"] = (gg.avg_price / gg.avg_price.cummax() - 1) * 100
        line(fig, gg, "dd", sym, COLORS[sym], fmt="+.2f", fill="tozeroy")
    fig.add_hline(y=0, line=dict(color=MUTED, width=1))
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
        for a, b in pairs:
            rc = piv[a].rolling(30, min_periods=10).corr(piv[b])
            rcdf = pd.DataFrame({"t": piv.index.tz_convert(TZ), "rc": rc.values})
            color = COLORS[b] if a == "BTCUSDT" else MUTED
            line(fig, rcdf, "rc", f"{a[:3]}–{b[:3]}", color, fmt=".2f")
        fig.add_hline(y=0, line=dict(color=MUTED, width=1))
        fig.update_yaxes(range=[-1, 1])
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
            fig = go.Figure(go.Histogram(x=r, nbinsx=40, marker=dict(color=COLORS[sym], line=dict(width=0)),
                                         hovertemplate="%{x:.3f}% · %{y} dk<extra></extra>", name=sym))
            for k in (-2, 2):
                fig.add_vline(x=k * sd, line=dict(color=WARN, width=1.5, dash="dot"))
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
    fig = go.Figure()
    for sym in symbols:
        gg = df[df.symbol == sym].dropna(subset=["ret_pct", "volume_usd"])
        fig.add_trace(go.Scatter(
            x=gg["volume_usd"], y=gg["ret_pct"], mode="markers", name=sym,
            marker=dict(color=COLORS[sym], size=7, opacity=0.55, line=dict(width=0)),
            customdata=gg["t"].dt.strftime("%d.%m %H:%M"),
            hovertemplate="%{customdata}<br>hacim $%{x:,.0f}<br>getiri %{y:+.3f}%<extra>" + sym + "</extra>"))
    if not al.empty:
        ap = al[al.kind == "ret_pct"].merge(df[["window_start", "symbol", "ret_pct", "volume_usd"]],
                                           on=["window_start", "symbol"], how="inner")
        if not ap.empty:
            fig.add_trace(go.Scatter(
                x=ap["volume_usd"], y=ap["ret_pct"], mode="markers", name="Spark fiyat uyarısı",
                marker=dict(color="rgba(0,0,0,0)", size=13, line=dict(color=NEG, width=2)),
                hovertemplate="z=%{customdata:+.1f}σ<extra>uyarı</extra>", customdata=ap["z"]))
    fig.update_xaxes(type="log", title="$ hacim / dk (log)",
                     tickvals=[1e3, 1e4, 1e5, 1e6, 1e7], ticktext=["$1K", "$10K", "$100K", "$1M", "$10M"])
    fig.add_hline(y=0, line=dict(color=MUTED, width=1))
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
                    marker=dict(color=COLORS[sym], symbol=shape, line=dict(color=SURFACE, width=1.5),
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
                                 marker=dict(color=COLORS[sym], line=dict(width=0)),
                                 customdata=np.stack([pp["trades"], pp["n"]], axis=1),
                                 hovertemplate="%{x}:00 · endeks %{y:.0f} · %{customdata[0]:,.0f} işlem/dk · %{customdata[1]} dk örnek<extra>" + sym + "</extra>"))
        fig.add_hline(y=100, line=dict(color=MUTED, width=1, dash="dot"))
        fig.update_layout(barmode="group", bargap=0.25)
        fig.update_xaxes(dtick=1, title="saat (TR)")
        st.plotly_chart(layout(fig, "aktivite endeksi (100 = coin'in ortalaması)", height=280), width="stretch", config=PLOTLY_CFG)
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

# =====================================================================
# 5) ML ANOMALİ — Isolation Forest ile çok değişkenli tespit
# =====================================================================
if section == "ML Anomali":
    # sklearn yalnızca bu bölümde gerekir; import burada ki diğer bölümler yavaşlamasın
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import RobustScaler

    st.subheader("Isolation Forest — çok değişkenli anomali tespiti")
    st.caption("Z-skor her seriyi tek tek inceler; Isolation Forest 5 özelliğe **birlikte** bakar: "
               "tek tek normal görünen ama kombinasyonu tuhaf olan dakikaları da yakalar.")

    mc1, mc2 = st.columns([2, 3])
    cont = mc1.slider("Beklenen anomali oranı (contamination)", 0.5, 5.0, 2.0, 0.5,
                      format="%.1f%%",
                      help="Modelin 'verinin yüzde kaçı anomalidir' varsayımı. "
                           "%2 ≈ saatte ~1 dakika. Z-skor |z|≥2 eşiği de kabaca %4.6'ya denk gelir.") / 100
    sym_ml = mc2.radio("Sembol", symbols, horizontal=True, key="ml_symbol")

    FEATURES = {
        "ret_pct":       "getiri % (dakikalık fiyat değişimi)",
        "range_pct":     "dk içi salınım % (max−min)/ort",
        "log_trades":    "log(işlem sayısı)",
        "log_volume":    "log($ hacim)",
        "avg_trade_usd": "ortalama işlem büyüklüğü $",
    }

    @st.cache_data(ttl=30)
    def iforest_scores(_df_key: tuple, contamination: float) -> pd.DataFrame:
        """Her sembol için ayrı model: özellik ölçekleri ve 'normal'i coin'e özgüdür
        (BTC'de 4.000 işlem/dk sıradan, SOL'da anomali olurdu)."""
        out = []
        for s, g in df.groupby("symbol", sort=False):
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
            g["if_flag"] = g["if_flag"].fillna(False)
            out.append(g)
        return pd.concat(out)

    # cache anahtarı: veri aralığı + satır sayısı (df'in kendisi hash'lenmesin diye)
    mldf = iforest_scores((minutes, len(df), str(df.window_start.max())), cont)
    g = mldf[mldf.symbol == sym_ml]
    col = COLORS[sym_ml]
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
                                 marker=dict(color=WARN, size=9, symbol="diamond",
                                             line=dict(color=SURFACE, width=1.5)),
                                 hovertemplate="z=%{customdata:+.1f}σ", customdata=zonly["z_ret"]))
    if not ifonly.empty:
        fig.add_trace(go.Scatter(x=ifonly["t"], y=ifonly["avg_price"], mode="markers",
                                 name="yalnız Isolation Forest",
                                 marker=dict(color=NEG, size=12, symbol="x-thin",
                                             line=dict(color=NEG, width=2.5)),
                                 hovertemplate="skor %{customdata:.3f}", customdata=ifonly["if_score"]))
    if not both_pts.empty:
        fig.add_trace(go.Scatter(x=both_pts["t"], y=both_pts["avg_price"], mode="markers",
                                 name="her ikisi (hemfikir)",
                                 marker=dict(color=POS, size=15, symbol="star",
                                             line=dict(color=SURFACE, width=1.5)),
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
    line(fig, g, "if_score", "anomali skoru", col, width=2, fmt=".3f")
    if thr_line is not None and pd.notna(thr_line):
        fig.add_hline(y=thr_line, line=dict(color=NEG, width=1, dash="dot"),
                      annotation_text=f"eşik ≈ {thr_line:.3f}", annotation_font_color=INK2)
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
