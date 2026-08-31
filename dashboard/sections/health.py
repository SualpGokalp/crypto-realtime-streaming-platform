"""Sağlık: boru hattının her halkasını yoklar (port, tazelik, Kafka akışı) ve
log dosyalarındaki hataları gösterir.

Amaç: "panel boş" dendiğinde hangi halkanın koptuğunu tahminle değil ölçümle
bulmak. İki seviye vardır: port yoklaması "süreç yaşıyor mu" (liveness),
tazelik/akış "işini yapıyor mu" (readiness) sorusudur — süreç ayakta ama
işlevsiz olabilir (ör. uyku sonrası Spark'ın heartbeat timeout'la ölmesi).
"""
import re
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st

from charts import explain
from data import PG_DSN, TZ


def _port_open(port: int) -> bool:
    import socket
    try:
        with socket.create_connection(("localhost", port), timeout=1.5):
            return True
    except OSError:
        return False


_LOG_TAIL_BYTES = 262_144   # dev log dosyasının tamamını okuma; son ~256 KB yeter


def _read_log(path: Path) -> list[str]:
    """Tee-Object UTF-16 (BOM'lu) yazar, python düz UTF-8; ikisini de okur.
    Yalnızca dosyanın sonu okunur — milyon satırlık eski loglar paneli kilitlemesin."""
    size = path.stat().st_size
    with path.open("rb") as f:
        head = f.read(2)
        enc = "utf-16-le" if head == b"\xff\xfe" else ("utf-16-be" if head == b"\xfe\xff" else "utf-8")
        if size > _LOG_TAIL_BYTES:
            off = size - _LOG_TAIL_BYTES
            if enc.startswith("utf-16"):
                off -= off % 2          # utf-16'da karakter ortasından bölmemek için çift hizala
            f.seek(off)
            lines = f.read().decode(enc, errors="replace").splitlines()
            return lines[1:]            # ilk satır büyük ihtimalle ortadan kesildi, at
        f.seek(0 if enc == "utf-8" else 2)
        return f.read().decode(enc, errors="replace").splitlines()


def render(raw: pd.DataFrame) -> None:
    st.subheader("Sistem sağlığı — zincirin hangi halkası kopuk?")

    # --- 1) port yoklamaları: süreç ayakta mı? ---
    kafka_up = _port_open(9094)
    pg_up = _port_open(5432)
    spark_up = _port_open(4040)

    # --- 2) veri tazeliği: consumer YAZIYOR mu? (ayakta olmak yetmez) ---
    fresh_sec, fresh_err = None, None
    if pg_up:
        try:
            with psycopg2.connect(PG_DSN) as conn:
                cur = conn.cursor()
                cur.execute("SELECT EXTRACT(EPOCH FROM (now() - MAX(updated_at))) FROM price_windows")
                v = cur.fetchone()[0]
                fresh_sec = float(v) if v is not None else None
        except Exception as e:
            fresh_err = str(e)

    # --- 3) Kafka akış hızı: producer YAZIYOR mu? (end offset artıyor mu?) ---
    flow_txt, flow_ok = "ölçülemedi", None
    if kafka_up:
        try:
            from kafka import KafkaConsumer, TopicPartition
            # request_timeout_ms, kütüphane kuralı gereği session_timeout_ms'ten
            # (varsayılan 10000) BÜYÜK olmalı; kafka-python 2.2'nin Consumer'ı
            # api_version_auto_timeout_ms parametresini tanımaz (Unrecognized configs)
            kc = KafkaConsumer(bootstrap_servers="localhost:9094",
                               request_timeout_ms=11000)
            parts = kc.partitions_for_topic("crypto-prices") or set()
            tps = [TopicPartition("crypto-prices", p) for p in parts]
            total = sum(kc.end_offsets(tps).values()) if tps else 0
            kc.close()
            now = pd.Timestamp.now()
            prev = st.session_state.get("_kafka_prev")
            st.session_state["_kafka_prev"] = (now, total)
            if prev is not None:
                dt = (now - prev[0]).total_seconds()
                rate = (total - prev[1]) / dt if dt > 0 else 0.0
                flow_ok = rate > 0
                flow_txt = f"{rate:,.1f} mesaj/sn · toplam {total:,}"
            else:
                flow_txt = f"toplam {total:,} mesaj (hız için bir sonraki yenilemeyi bekle)"
        except Exception as e:
            flow_ok, flow_txt = False, f"okunamadı: {type(e).__name__}"

    # --- durum tablosu + çözüm önerileri ---
    consumer_ok = spark_up and fresh_sec is not None and fresh_sec < 90
    rows = [
        ("PostgreSQL (5432)", pg_up,
         "bağlantı tamam" if pg_up else "porta ulaşılamıyor",
         "Docker Desktop'ı aç (yeşil olsun) → `docker compose up -d kafka postgres`"),
        ("Kafka (9094)", kafka_up,
         "bağlantı tamam" if kafka_up else "porta ulaşılamıyor",
         "Docker Desktop'ı aç → `docker compose up -d kafka postgres`"),
        ("Producer → Kafka akışı", flow_ok if flow_ok is not None else kafka_up, flow_txt,
         "PRODUCER penceresi kapanmış olabilir → `.venv\\Scripts\\python producer\\binance_producer.py`"),
        ("Spark consumer (4040)", spark_up,
         "Spark UI açık" if spark_up else "Spark UI kapalı — consumer çalışmıyor",
         "CONSUMER penceresini yeniden başlat → `powershell -ExecutionPolicy Bypass -File run_consumer.ps1`"),
        ("Veri tazeliği", consumer_ok,
         (f"son yazma {fresh_sec:.0f} sn önce" if fresh_sec is not None
          else (fresh_err or "tablo boş — hiç veri yazılmamış")),
         "Consumer açık ama yazmıyorsa 1-2 dk bekle (birikmiş veriyi yakalıyordur); "
         "sürerse CONSUMER penceresindeki son satırlara/log'a bak"),
    ]
    broken = [r for r in rows if not r[1]]
    if not broken:
        st.success("Tüm halkalar sağlam: Binance → Kafka → Spark → PostgreSQL → panel", icon="✅")
    else:
        st.error(f"{len(broken)} halka sorunlu — aşağıdaki çözüm sütununa bak", icon="🚨")
    st.dataframe(pd.DataFrame(
        [{"Bileşen": n, "Durum": "✅" if ok else "❌", "Detay": d, "Çözüm (sorunluysa)": fix}
         for n, ok, d, fix in rows]), width="stretch", hide_index=True)
    explain("Kontroller nasıl çalışıyor?", """
| Kontrol | Yöntem | Neyi kanıtlar |
|---|---|---|
| Port yoklaması | `localhost:9094/5432/4040`'a TCP bağlantısı | Süreç **ayakta** (ama iş yapıyor demek değil) |
| Producer akışı | Kafka'nın `end_offsets`'i iki yenileme arasında artıyor mu | Producer **gerçekten yazıyor** |
| Veri tazeliği | `MAX(updated_at)` şu andan ne kadar geride | Consumer **gerçekten yazıyor** |

İki seviye var çünkü süreç **ayakta ama işlevsiz** olabilir (yaşanmış örnek: consumer
penceresi açık görünürken Spark içeride heartbeat timeout ile çökmüştü). Port yoklaması
"yaşıyor mu", tazelik/akış "işini yapıyor mu" sorusudur — üretim sistemlerinde buna
*liveness* vs *readiness* denir.
""")

    # --- log hataları ---
    st.subheader("Log dosyalarındaki hatalar")
    log_dir = Path(__file__).resolve().parents[2] / "logs"
    pat = re.compile(r"ERROR|Exception|Traceback|CRITICAL|Py4JJavaError|ConnectionRefused", re.I)
    # PowerShell'in Tee-Object'i stderr satırlarını "NativeCommandError/RemoteException"
    # süslemesiyle sarar — bunlar gerçek hata değil, kabuk gürültüsüdür; eleriz
    noise = re.compile(r"NativeCommandError|RemoteException|CategoryInfo|FullyQualifiedErrorId|^\s*\+")
    logs = sorted(log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True) if log_dir.exists() else []
    if not logs:
        st.info("`logs/` klasöründe dosya yok. Producer/consumer'ı log tutarak başlatırsan "
                "(başlatma rehberindeki `Tee-Object` komutları) hatalar burada görünür.")
    for lf in logs:
        mtime = pd.Timestamp(lf.stat().st_mtime, unit="s", tz="UTC").tz_convert(TZ)
        try:
            lines = _read_log(lf)
        except OSError as e:
            st.warning(f"{lf.name} okunamadı: {e}")
            continue
        errs = [l.strip() for l in lines if pat.search(l) and not noise.search(l)]
        icon = "🔴" if errs else "🟢"
        with st.expander(f"{icon} {lf.name} — {len(errs)} hata satırı · son yazma {mtime:%d.%m %H:%M} · {len(lines):,} satır"):
            if errs:
                st.code("\n".join(errs[-15:]), language="text")
                st.caption("Son 15 hata satırı. Tam bağlam için dosyayı aç: " + str(lf))
            else:
                st.caption("Hata deseni yok. Dosyanın son 5 satırı (canlılık kontrolü):")
                st.code("\n".join(l.strip() for l in lines[-5:]), language="text")
    explain("Hata satırları nasıl bulunuyor, nasıl okunur?", """
Her `.log` dosyasında `ERROR / Exception / Traceback / CRITICAL / Py4JJavaError /
ConnectionRefused` desenleri aranır (büyük-küçük harf duyarsız).

- **Py4JJavaError**: PySpark hatası — asıl sebep genelde birkaç satır aşağıda
  "Caused by:" ile başlar; log dosyasını açıp onu bul.
- **ConnectionRefused**: bir halka kapalıyken diğeri ona bağlanmaya çalışmış
  (ör. Kafka kapalıyken producer). Üstteki durum tablosu hangi halka olduğunu söyler.
- WARN satırları bilerek dahil edilmez: Spark açılışta düzinelerce zararsız WARN basar
  (örn. "state doesn't exist in loadedMaps" — ilk batch'te normaldir).

Not: pencereyi log tutmadan başlattıysan çıktı yalnız ekranda kalır ve pencere kapanınca
kaybolur — consumer'ın ilk çöküşünde sebebini bu yüzden görememiştik. Başlatma
rehberindeki `Tee-Object`'li komutlar hem ekrana hem dosyaya yazar.
""")
