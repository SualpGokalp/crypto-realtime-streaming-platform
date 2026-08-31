"""Panelin tüm bölümlerini headless çalıştırıp hata tarar.

Çalıştır:  .venv\\Scripts\\python tests\\test_sections.py
(Canlı Postgres gerekir; veri yoksa bölümler sağlık ekranına düşer, bu da geçerlidir.)
"""
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")
SECTIONS = ["Genel bakış", "Sembol detay", "Derin analiz", "Anomaliler & veri", "ML Anomali", "Sağlık"]


def main() -> int:
    failed = 0
    for sec in SECTIONS:
        at = AppTest.from_file(APP, default_timeout=120)
        at.query_params["section"] = sec
        at.run()
        if at.exception:
            print(f"{sec}: EXCEPTION -> {at.exception[0].value}")
            failed += 1
        else:
            print(f"{sec}: OK")
    print("HEPSI GECTI" if not failed else f"{failed} BOLUM HATALI")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
