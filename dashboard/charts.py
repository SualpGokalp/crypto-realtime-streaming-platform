"""Grafik yardımcıları: ortak Plotly düzeni, çizgi/çubuk ekleme, açıklama kutusu.

Renkler her çağrıda `theme.current()`'tan okunur; böylece tema değişince
grafikler de değişir (modül seviyesinde renk sabitlenmez).
"""
import plotly.graph_objects as go
import streamlit as st

import theme

PLOTLY_CFG = {"displayModeBar": False, "scrollZoom": False}


def layout(fig: go.Figure, ytitle: str = "", height: int = 300, legend: bool = True) -> go.Figure:
    th = theme.current()
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=8, b=40), showlegend=legend,
        uirevision="keep",   # yenilemede kullanıcının zoom/legend seçimlerini koru

        paper_bgcolor=th.SURFACE, plot_bgcolor=th.SURFACE,
        font=dict(color=th.INK, size=12), hovermode="x unified",
        hoverlabel=dict(bgcolor=th.SURFACE, bordercolor=th.GRID,
                        font=dict(color=th.INK, size=12)),
        barcornerradius=4,   # çubuk uçları hafif yuvarlak (veri ucu vurgusu)
        legend=dict(orientation="h", y=1.04, x=0, font=dict(color=th.INK2)),
        xaxis=dict(showgrid=False, linecolor=th.GRID, tickfont=dict(color=th.INK2),
                   tickformat="%H:%M", hoverformat="%d.%m %H:%M",
                   # crosshair: imlecin olduğu dakikada dikey kılavuz çizgisi
                   showspikes=True, spikemode="across", spikesnap="cursor",
                   spikecolor=th.MUTED, spikethickness=1, spikedash="dot"),
        yaxis=dict(title=ytitle, gridcolor=th.GRID, zeroline=False,
                   tickfont=dict(color=th.INK2), title_font=dict(color=th.INK2)),
    )
    return fig


def line(fig, g, col, name, color, width=2, dash=None, fmt=",.2f", fill=None):
    fig.add_trace(go.Scatter(
        x=g["t"], y=g[col], name=name, mode="lines",
        line=dict(color=color, width=width, dash=dash), fill=fill,
        fillcolor=theme.rgba(color, 0.12) if fill else None,   # dolgu, çizginin saydam tonu
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
