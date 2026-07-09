import json
import websocket
from kafka import KafkaProducer
from datetime import datetime, timezone

# Kafka'ya bağlan (senin bilgisayarından → localhost:9094)
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    """Binance'ten her mesaj gelince çalışır."""
    data = json.loads(message)
    
    # Sadece işimize yarayan alanları al
    record = {
        "symbol":    data["s"],           # BTC/USDT
        "price":     float(data["p"]),    # işlem fiyatı
        "quantity":  float(data["q"]),    # işlem miktarı
        "timestamp": data["T"],           # işlem zamanı (ms)
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Kafka'ya yaz
    producer.send('crypto-prices', value=record)
    print(f"[{record['ingested_at']}] BTC: ${record['price']:,.2f}")

def on_error(ws, error):
    print(f"Hata: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Bağlantı kapandı")

def on_open(ws):
    print("Binance'e bağlandı, fiyatlar akıyor...")

# WebSocket başlat
ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

ws.run_forever()