"""Tema ve renk paleti.

Renkler doğrulanmış bir veri görselleştirme paletinden gelir (renk körlüğü
simülasyonu + kontrast testlerinden geçmiş): kategorik seriler sabit sıralı
mavi/turuncu/yeşil, durum renkleri ayrı, kutuplu veriler mavi↔kırmızı diverging.

ÖNEMLİ: `current()` her rerun'da çağrılmalı, modül seviyesinde sabitlenmemeli —
Streamlit modülü bir kez import eder ama tema (açık/koyu) oturumdan oturuma ve
kullanıcı değiştirince rerun'dan rerun'a değişir.
"""
from types import SimpleNamespace

import streamlit as st


def rgba(hex_color: str, alpha: float) -> str:
    """'#2a78d6' → 'rgba(42,120,214,0.1)' — seri renginde saydam dolgu için."""
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{alpha})"


def current() -> SimpleNamespace:
    """O anki temaya (açık/koyu) göre tüm renk sabitlerini üretir."""
    dark = st.context.theme.type == "dark"
    return SimpleNamespace(
        DARK=dark,
        # sabit sıra: coin → renk hiç değişmez
        COLORS=({"BTCUSDT": "#3987e5", "ETHUSDT": "#d95926", "SOLUSDT": "#199e70"} if dark
                else {"BTCUSDT": "#2a78d6", "ETHUSDT": "#eb6834", "SOLUSDT": "#1baf7a"}),
        SURFACE="#1a1a19" if dark else "#fcfcfb",
        GRID="#383835" if dark else "#e6e5e1",
        INK="#ffffff" if dark else "#0b0b0b",
        INK2="#c3c2b7" if dark else "#52514e",
        MUTED="#6f6e69" if dark else "#9a9892",   # yardımcı çizgiler (SMA, bant)
        POS="#0ca30c",                             # durum: iyi / pozitif
        NEG="#d03b3b",                             # durum: kritik / negatif
        WARN="#fab219",                            # durum: uyarı (anomali işareti)
        # diverging (kutuplu) skala: korelasyon gibi -1..+1 veriler için mavi ↔
        # kırmızı, ortada nötr gri. Turuncu kullanılmaz: ETH'nin rengiyle karışırdı.
        DIV_POS="#3987e5" if dark else "#2a78d6",
        DIV_NEG="#e66767" if dark else "#e34948",
        DIV_MID="#383835" if dark else "#f0efec",
    )


def inject_css(th: SimpleNamespace) -> None:
    """Streamlit bileşenlerine ince rötuş: metrik kartlarına yüzey + çerçeve,
    böylece sayılar boşlukta yüzmez."""
    st.markdown(f"""<style>
[data-testid="stMetric"] {{
    background: {th.SURFACE}; border: 1px solid {th.GRID}; border-radius: 10px;
    padding: 10px 14px;
}}
[data-testid="stMetric"] [data-testid="stMetricLabel"] p {{ color: {th.INK2}; }}
</style>""", unsafe_allow_html=True)
