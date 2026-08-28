import json
import time
import websocket
from kafka import KafkaProducer
from datetime import datetime, timezone

# Kafka'ya bağlan (senin bilgisayarından → localhost:9094)
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

RECONNECT_DELAY = 5  # bağlantı kopunca kaç saniye bekleyip tekrar denenecek

# Dinlenecek pariteler. Binance "combined stream" ile hepsi TEK websocket'ten gelir.
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{s}@trade" for s in SYMBOLS
)

def on_message(ws, message):
    """Binance'ten her mesaj gelince çalışır."""
    # Combined stream'de mesaj bir kat sarılı gelir: {"stream": "btcusdt@trade", "data": {...}}
    data = json.loads(message)["data"]

    # Sadece işimize yarayan alanları al
    record = {
        "symbol":    data["s"],           # BTCUSDT / ETHUSDT / SOLUSDT
        "price":     float(data["p"]),    # işlem fiyatı
        "quantity":  float(data["q"]),    # işlem miktarı
        "timestamp": data["T"],           # işlem zamanı (ms)
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Kafka'ya yaz
    producer.send('crypto-prices', value=record)
    print(f"[{record['ingested_at']}] {record['symbol']}: ${record['price']:,.2f}")

def on_error(ws, error):
    print(f"Hata: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Bağlantı kapandı")

def on_open(ws):
    print(f"Binance'e bağlandı, akan pariteler: {', '.join(x.upper() for x in SYMBOLS)}")

# WebSocket başlat
ws = websocket.WebSocketApp(
    STREAM_URL,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

# run_forever() bağlantı kopunca geri döner; döngüye alarak otomatik yeniden
# bağlanma sağlıyoruz. Ctrl+C ile temiz şekilde çıkılır.
try:
    while True:
        ws.run_forever()
        print(f"Bağlantı koptu, {RECONNECT_DELAY} sn sonra tekrar bağlanılıyor...")
        time.sleep(RECONNECT_DELAY)
except KeyboardInterrupt:
    print("Durduruluyor...")
finally:
    producer.flush()   # Kafka'ya gönderilmeyi bekleyen mesajları teslim et
    producer.close()
