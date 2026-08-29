# Kod Rehberi — satır satır ne, ne işe yarıyor

Bu belge projeyi bir ay sonra açınca "burada ne yapmıştık?" sorusuna cevap vermek için yazıldı.
Dosyalar **verinin aktığı sırayla** anlatılıyor: Binance → producer → Kafka → Spark → Postgres → panel.

```
Binance ─► producer.py ─► Kafka [crypto-prices] ─► spark_consumer.py ─┬─► Postgres price_windows ─► dashboard/app.py
                                                                       └─► alerts.py ─► Kafka [crypto-alerts] + Postgres alerts
```

---

## 0. docker-compose.yml — altyapı

Üç konteyner tanımlar. `docker compose up -d kafka postgres` ile ikisi kalkar.

**kafka** — `apache/kafka:latest`, KRaft modunda (ZooKeeper yok; Kafka kendi kendini yönetir).
- `KAFKA_PROCESS_ROLES: broker,controller` → tek konteyner hem veri tutar (broker) hem kümeyi yönetir (controller).
- Üç *listener* (dinleme adresi) var, bu kısım kafa karıştırır:
  - `PLAINTEXT://kafka:9092` → **konteynerler arası** (Kafka UI buradan bağlanır).
  - `CONTROLLER://9093` → Kafka'nın kendi iç yönetim kanalı.
  - `PLAINTEXT_HOST://localhost:9094` → **senin bilgisayarın** (producer ve Spark buraya bağlanır).
  Neden iki farklı adres? Kafka istemciye "beni şu adresten bul" der (`ADVERTISED_LISTENERS`). Konteyner içinden `kafka:9092` çalışır ama Windows'tan `kafka` diye bir makine yok; o yüzden dışarıya `localhost:9094` ilan edilir.
- `..._REPLICATION_FACTOR: 1` → tek broker var, kopya tutamaz.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` → producer `crypto-prices`'a yazınca topic kendiliğinden oluşur.
- `volumes: kafka_data:/var/lib/kafka/data` → topic verisi ve offset'ler konteyner silinse de kalır. (Bu satır 29.08'e kadar eksikti; her Kafka restart'ında offset sıfırlanıyor ve Spark "offset changed" hatasıyla çöküyordu.)

**kafka-ui** — tarayıcıdan topic/mesaj görmek için (`localhost:8081`). ~400 MB RAM yer, gerekmedikçe açma.

**postgres** — `postgres:16`, kullanıcı/şifre/db hepsi `crypto`.
- `postgres_data` volume → veriler kalıcı.
- `./db/init.sql` → konteyner **ilk kez** oluşturulurken çalışır, tabloları kurar. Sonraki açılışlarda çalışmaz (volume zaten var).

## 0b. db/init.sql — tablolar

**price_windows** — Spark'ın her 1 dk penceresi için yazdığı satır.
- `window_start, window_end` (TIMESTAMPTZ, UTC) — dakikanın başı/sonu.
- `symbol` — BTCUSDT / ETHUSDT / SOLUSDT.
- `avg_price, min_price, max_price, trade_count, total_volume` — o dakikanın 5 özeti.
- `updated_at` — satır en son ne zaman güncellendi (panel "canlı mı" kontrolünü bununla yapar).
- `PRIMARY KEY (window_start, symbol)` → aynı dakika + aynı sembol tek satırdır. Spark aynı pencereyi birkaç kez gönderir (aşağıda), bu anahtar sayesinde `ON CONFLICT` ile üstüne yazılır.

**alerts** — anomali uyarıları. `kind` = hangi seri (ret_pct / volume_usd / trade_count), `value` o dakikanın değeri, `baseline` önceki 30 dk ortalaması, `sigma` sapması, `z` skor. `UNIQUE (window_start, symbol, kind)` → consumer yeniden başlayıp aynı pencereyi tekrar görürse ikinci kez yazmaz.

---

## 1. producer/binance_producer.py — Binance'ten Kafka'ya

```python
producer = KafkaProducer(bootstrap_servers='localhost:9094',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
```
Kafka'ya bağlanan nesne. Kafka **byte** taşır, Python dict taşımaz; `value_serializer` her mesajı dict → JSON metni → byte'a çevirir.

```python
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
STREAM_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
```
Binance'in *combined stream* adresi: üç pariteyi tek websocket bağlantısından alırız. `@trade` = her gerçekleşen alım-satım işlemi (saniyede onlarca).

```python
def on_message(ws, message):
    data = json.loads(message)["data"]
```
Combined stream mesajı bir kat sarılı gelir: `{"stream": "btcusdt@trade", "data": {...}}`. İçindeki `data`'yı alırız. Binance alan adları tek harf: `s` sembol, `p` fiyat, `q` miktar, `T` işlem zamanı (milisaniye epoch).

```python
    record = {"symbol": data["s"], "price": float(data["p"]), "quantity": float(data["q"]),
              "timestamp": data["T"], "ingested_at": datetime.now(timezone.utc).isoformat()}
    producer.send('crypto-prices', value=record)
```
Sadece işimize yarayan 4 alan + bizim aldığımız an (`ingested_at`, gecikme ölçmek için). `send` **asenkron**: mesaj bir tampona gider, arka planda toplu yollanır — bu yüzden hızlıdır.

`on_open / on_error / on_close` — bağlantı olaylarında konsola yazan basit fonksiyonlar.

```python
while True:
    ws.run_forever()          # bağlantı kopana kadar bloklar
    time.sleep(RECONNECT_DELAY)
```
`run_forever` bağlantı kopunca geri döner; döngü sayesinde 5 sn sonra tekrar bağlanır. `Ctrl+C` → `KeyboardInterrupt` → `finally:` bloğunda `producer.flush()` (tampondaki mesajları teslim et) ve `close()`.

---

## 2. consumer/spark_consumer.py — Kafka'dan oku, dakikaya böz, kaydet

### Bölüm 0 — ortam değişkenleri
Spark Java üstünde çalışır (JVM). PySpark, `JAVA_HOME`'u bulamazsa hiç başlamaz; Windows'ta ayrıca `HADOOP_HOME\bin\winutils.exe` gerekir (dosya sistemi işlemleri için). Bu blok, terminalde ayarlı değilse bilinen kurulum yollarını yazar ve `bin` klasörlerini `PATH`'e ekler. **`pyspark` import'undan önce** olmak zorunda; import anında JVM başlar.

### Bölüm 1 — SparkSession
`SparkSession` Spark'a açılan kapı; tüm ayarlar burada.
- `.master("local[2]")` → küme yok, bu bilgisayarda 2 çekirdek. `local[*]` hepsini alıp makineyi kastırıyordu.
- `spark.driver.memory 1g` → JVM en fazla 1 GB.
- `spark.jars.packages ... spark-sql-kafka-0-10_2.12:3.5.6` → Spark'ın Kafka'yı okumayı bilmesi için gereken eklenti; ilk çalıştırmada Maven'dan indirir. Sürüm pyspark sürümüyle (3.5.6) **aynı** olmalı.
- `fs.file.impl RawLocalFileSystem` + `checkpointFileManagerClass FileSystemBased...` → Windows'a özel: Spark dosya yazarken `.crc` sağlama dosyaları oluşturup yeniden adlandırır, Windows'ta bu patlar. Sağlamasız dosya sistemine zorluyoruz.
- `spark.sql.shuffle.partitions 4` → `groupBy` yaparken veriyi kaç parçaya böler. Varsayılan 200; lokalde 4 yeter, fazlası yavaşlatır.
- `spark.sql.session.timeZone UTC` → tüm zaman hesapları UTC. Binance zamanı UTC, makine TR (+3); sabitlemezsek pencere sınırları kayar.

### Bölüm 2 — readStream
```python
raw_stream = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false").load()
```
`readStream` = **sonu olmayan tablo**. Kafka'dan gelen her mesaj bir satırdır; sütunları `key, value (byte), topic, partition, offset, timestamp`. `startingOffsets latest` → checkpoint yoksa eski mesajları atla, şimdiden başla. `failOnDataLoss false` → checkpoint'te hatırlanan offset Kafka'da artık yoksa (silinmiş/sıfırlanmış) hata verip durmak yerine devam et.

### Bölüm 3-4 — şema ve parse
Kafka'daki `value` sadece byte. Spark'a "içinde şu alanlar var" demek gerekir (`StructType`). Sonra:
```python
.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
```
byte → string → JSON'dan sütunlara (`data.*` = iç alanları dışarı çıkar). `timestamp` milisaniye; 1000'e bölüp `timestamp` tipine çevirince gerçek tarih-saat olur. Buna **event time** denir: işlemin Binance'te gerçekleştiği an (bizim aldığımız an değil).

### Bölüm 5 — pencereleme (projenin kalbi)
```python
windowed = parsed_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
    .agg(avg("price"), min("price"), max("price"), count("*"), sum("quantity"))
```
- `window(event_time, "1 minute")` → her satırı ait olduğu dakikaya koyar (12:03:17 → [12:03, 12:04)).
- `groupBy(pencere, sembol)` → aynı dakika + aynı sembol satırları bir grup.
- `agg` → grup başına 5 özet.
- **Watermark**: "Bir dakikanın son işlemi geldikten sonra 30 sn daha geç gelenleri kabul et, sonra o dakikayı kapat." Bunsuz Spark her dakikayı sonsuza kadar hafızada tutmak zorunda kalırdı (belki 3 saat sonra o dakikaya ait bir mesaj gelir diye). Watermark = *bellek sınırı + "kesinleşme" tanımı*.

Sonraki `.select` sadece `window.start` / `window.end`'i düz sütunlara açar.

### Bölüm 6 — Postgres'e yaz (sorgu 1, update modu)
Spark'ın hazır JDBC yazıcısı sadece INSERT bilir; bize **upsert** ("varsa güncelle") lazım. Neden? `outputMode("update")` bir pencereyi dakika dolmadan da gönderir: 12:03 penceresi için 12:03:30'da (yarım) bir satır, 12:04:00'da (tam) bir satır daha. Aynı satır iki kez gelir, ikincisi birincinin üstüne yazılmalı.

Çözüm `foreachBatch`: Spark her tetiklemede (30 sn) o anki değişen satırları bir DataFrame olarak bizim Python fonksiyonumuza verir; biz istediğimizi yaparız.

```python
def write_to_postgres(batch_df, batch_id):
    rows = [(_utc(r.window_start), ...) for r in batch_df.collect()]
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, template="(%s, ..., now())")
```
- `collect()` → satırları driver'a (Python'a) çeker; satır sayısı küçük (3-6) olduğundan güvenli.
- `_utc()` → PySpark `collect` ile zamanı **saat dilimsiz** (naive) ve makinenin yerel saatinde verir. Öyle bırakırsak Postgres onu UTC sanır → 3 saat kayar. `astimezone(timezone.utc)` düzeltir.
- `execute_values` → tek SQL'de çok satır INSERT; `ON CONFLICT (window_start, symbol) DO UPDATE` upsert'i yapar.

```python
query = windowed.writeStream.outputMode("update").foreachBatch(write_to_postgres)
    .option("checkpointLocation", "checkpoint/price_windows").trigger(processingTime="30 seconds").start()
```
- `checkpointLocation` → Spark "Kafka'da nereye kadar okudum, hangi pencereler açık" bilgisini buraya diske yazar; program kapanıp açılınca kaldığı yerden devam eder.
- `trigger 30 seconds` → her 30 sn'de bir mikro-batch.

### Bölüm 7 — anomali (sorgu 2, append modu)
Aynı `windowed` DataFrame'e ikinci bir `writeStream`. Farkı `outputMode("append")`: pencere **yalnızca kesinleşince** (watermark geçince) ve **tek sefer** gelir. Yarım pencereye z-skor uygularsak "işlem sayısı yarıya düştü" diye sahte alarm olurdu; append modu bunu önler. Bedeli: tespit 30-90 sn gecikir.

`detect_anomalies(batch_df, batch_id)` → satırları toplar, `alerts.detect()`'e verir, bulunanları konsola yazar. Ayrı `checkpoint/alerts` klasörü (her sorgunun kendi hafızası olmalı). En sonda `spark.streams.awaitAnyTermination()` → iki sorgu paralel koşar; biri hata verirse program biter.

---

## 3. consumer/alerts.py — z-skor hesabı

Sabitler: `Z_THRESHOLD` (env `ALERT_Z`, varsayılan 2.0), `HISTORY_MIN` (30 dk), `MIN_HISTORY` (5 — daha az geçmişle sapma güvenilmez, hesaplama).

`ensure_table()` → `CREATE TABLE IF NOT EXISTS alerts` (init.sql eski volume'da çalışmadığı için burada da var).

`_history(cur, symbol, window_start)` → Postgres'ten bu pencereden **önceki** en fazla 31 dakikanın `avg_price, trade_count, total_volume` değerleri, eskiden yeniye. Zaman sınırı var ki consumer 3 gün kapalı kaldıysa 3 gün öncesiyle karşılaştırmasın.

`_series(prices, trades, volumes)` → üç seri üretir:
- `ret_pct` = ardışık dakikalar arası fiyat değişimi %, `(p1 − p0) / p0 × 100`
- `trade_count` = olduğu gibi
- `volume_usd` = `miktar × fiyat`

`score_window(cur, row)`:
1. geçmişi çek; 5'ten azsa `[]` dön.
2. geçmiş + bu pencere → seriler.
3. her seri için `past = values[:-1]`, `current = values[-1]` — **kendisi ortalamaya dahil edilmez**, yoksa aykırı değer kendi eşiğini şişirir.
4. `mu = fmean(past)`, `sd = pstdev(past)`, `z = (current − mu) / sd`. `sd == 0` ise atla (bölme hatası).
5. `|z| ≥ eşik` → uyarı sözlüğü.

`publish(alerts)` → önce Kafka: `prod.send("crypto-alerts", key=symbol, value=a)` (key = sembol → aynı sembolün uyarıları aynı partition'a, sıralı). `flush()` teslimi bekler. Sonra Postgres `INSERT ... ON CONFLICT DO NOTHING`.

`detect(rows)` → her satır için `score_window`, hepsini `publish`. `_kafka()` producer'ı ilk ihtiyaçta bir kez kurar (global), her batch'te yeniden bağlanmamak için.

---

## 4. dashboard/app.py — panel

Streamlit modeli: **dosya yukarıdan aşağı her etkileşimde baştan çalışır.** Kaydırıcıyı oynatınca, 30 sn dolunca, bölüm değişince → tüm script yeniden koşar. Bunu bilince kodun yapısı anlaşılır.

### Tema & renkler
`st.context.theme.type` → kullanıcının tarayıcı teması. Koyu/açık için iki palet; coin renkleri sabit (BTC mavi, ETH turuncu, SOL yeşil), durum renkleri `POS/NEG/WARN`.

### Veri fonksiyonları
`@st.cache_data(ttl=10)` → fonksiyon aynı argümanla 10 sn içinde tekrar çağrılırsa DB'ye gitmez, önceki sonucu verir. Script her etkileşimde baştan koştuğu için bu şart.

`load(minutes)` → `price_windows`'tan son N dakika. `t` sütunu = `window_start` TR saatine çevrilmiş (sadece görüntü için; hesaplar UTC).

`load_alerts(minutes)` → `alerts` tablosu; tablo yoksa boş DataFrame.

`enrich(df, ...)` → sembol sembol türetilmiş göstergeler. Hepsi pandas `rolling` (kayan pencere):
- `volume_usd = total_volume × avg_price`, `avg_trade_usd = volume_usd / trade_count`
- `ret_pct = pct_change × 100` (dakikalık getiri)
- `range_pct = (max − min) / avg × 100` (dakika içi salınım)
- `sma_s / sma_l = rolling(N).mean()`, `ema_s = ewm(span).mean()` (üstel ağırlıklı)
- Bollinger: `bb_mid = rolling(20).mean()`, `bb_sd = rolling(20).std()`, üst/alt = `mid ± 2·sd`
- `vol_pct = ret_pct.rolling(30).std()` (oynaklık)
- `index100 = p / p.iloc[0] × 100` (ilk dakikaya endeks), `cum_ret_pct = index100 − 100`
- `vwap = cumsum(p × hacim) / cumsum(hacim)`
- z-skorlar: `mu = rolling(30).mean().shift(1)` — `shift(1)` = **bir önceki satıra kadar**, yani kendisi hariç. `z = (x − mu) / sd`.
`min_periods` → en az kaç veri olmadan hesaplama (başta NaN kalır).

### Grafik yardımcıları
`PLOTLY_CFG` → Plotly araç çubuğunu gizle, scroll ile zoom kapalı.
`layout(fig, ...)` → her grafiğe aynı görünüm: yükseklik, kenar boşluğu, tema renkleri, `hovermode="x unified"` (imleçteki dakikanın tüm serilerini tek kutuda göster), `uirevision="keep"` (yenilemede zoom/legend durumunu koru).
`line()` / `bars()` → tek satırda çizgi/çubuk ekleme; `hovertemplate` sayı biçimi.
`explain(title, body)` → grafiğin altındaki "ⓘ nasıl hesaplanıyor" açılır kutusu.

### Üst şerit
Başlık, 4 kontrol: zaman aralığı (30 dk … 24 sa), kısa/uzun SMA uzunluğu, oto-yenile. `st_autorefresh(30_000)` → 30 sn'de bir script'i yeniden koşturur. Sonra `load` + `enrich`; veri yoksa uyarı verip durur. `age` = son yazmadan bu yana geçen sn; 90'ı aşarsa "consumer durmuş olabilir".

`segmented_control` → üç bölümden sadece seçili olan çizilir (`st.tabs` gizli sekmeleri de çiziyordu → 3 kat yük).

### Bölüm 1 — Genel bakış
- **Kartlar**: `st.metric` sembol başına son fiyat + aralık başından beri getiri.
- **100'e endeks**: her sembolün `index100` çizgisi + y=100 referans çizgisi. Farklı fiyat seviyelerini (77.000 $ vs 180 $) yüzde olarak aynı eksende karşılaştırır.
- **Korelasyon**: `pivot_table` ile satır=dakika, sütun=sembol, değer=getiri tablosu; `.corr()` Pearson; `go.Heatmap` renkli matris.
- **Aralık özeti**: sembol başına min/max/oynaklık/hacim tablosu, `style.format` ile biçim.
- **Dolar hacmi**: yığılmış çubuk (`barmode="stack"`), dakika başına coin'lerin hacim payı.

### Bölüm 2 — Sembol detay
- `st.radio` sembol seçimi, 5 metrik (fiyat, VWAP, oynaklık, işlem/dk, ort. işlem).
- **Fiyat grafiği**: `st.pills("Katmanlar", key="detail_layers")` → hangi katmanların çizileceği; `key` sayesinde seçim yenilemede korunur. Katmanlar: min-max bandı (`fill="tonexty"` iki çizgi arasını boyar), Bollinger, SMA kısa/uzun, VWAP, ortalama fiyat (her zaman), anomali elmasları (`z_ret ≥ 2` olan dakikalar, `mode="markers"`).
- **Dakikalık getiri**: çubuk; renk `np.where(ret ≥ 0, POS, NEG)`.
- **Oynaklık**: `vol_pct` (30 dk σ) + `range_pct`.
- **İşlem sayısı**: çubuk + 30 dk hareketli ortalama çizgisi.
- **Ort. işlem $**: çizgi.

### Bölüm "Derin analiz"
"Ne oldu"dan "ne kadar sıra dışı / neden"e geçen grafikler. Hepsi `df`'ten (enrich çıktısı) ve `alerts` tablosundan türetilir:
- **Zirveden düşüş**: `avg_price / avg_price.cummax() − 1` — aralık zirvesinden % kayıp; üç coin aynı % ekseninde.
- **Hareketli korelasyon**: `piv[a].rolling(30).corr(piv[b])` — Genel bakıştaki tek sayının zaman içindeki hali; düşüş = ayrışma.
- **Getiri dağılımı**: sembol başına histogram, ±2σ çizgileri; eksen altındaki %, eşiği aşan dakika payı (normalde ~%4.6).
- **Hareket–hacim**: x = `volume_usd` (log), y = `ret_pct`; kırmızı halka = Spark fiyat uyarısı olan dakika.
- **Uyarı zaman çizelgesi**: y = sembol, şekil = tür (▲ fiyat ■ işlem ● hacim), boyut = |z|.
- **Saatlik aktivite**: `load_profile()` son 7 günü saat başına gruplar; her coin kendi ortalamasına endekslenir (100 = sıradan saat).
URL ile bölüm açılabilir: `?section=Derin%20analiz` (`st.query_params`).

### Bölüm 3 — Anomaliler & veri
- Eşik kaydırıcısı; `df` içinde `|z_ret| ≥ thr` veya `|z_trades|` veya `|z_vol|` olan satırlar; `np.select` ile "neden" etiketi.
- **Spark uyarıları**: `load_alerts` tablosu; "Gecikme" = `detected_at − window_start − 60 sn` (pencere bitiminden tespite).
- **Ham + türetilmiş veri**: tüm sütunlar tablo + CSV indir.

---

## Sık karışan kavramlar — tek cümle

| Kavram | Anlamı |
|---|---|
| topic | Kafka'da mesaj kuyruğunun adı (`crypto-prices`, `crypto-alerts`) |
| offset | Kafka'da mesajın sıra numarası; consumer "kaçıncıya kadar okudum"u bununla tutar |
| event time | işlemin gerçekleştiği an (Binance saati); processing time = bizim işlediğimiz an |
| watermark | "bu kadar geç gelen veriyi hâlâ kabul ederim" sınırı; pencerelerin ne zaman kesinleşeceğini belirler |
| update / append | update: pencere her değişince gönder; append: sadece kesinleşince, tek sefer |
| micro-batch / trigger | Spark akışı 30 sn'lik küçük paketler halinde işler |
| foreachBatch | her mikro-batch'i kendi Python fonksiyonuna verme yolu |
| checkpoint | Spark'ın "nerede kaldım" defteri (disk) |
| upsert | varsa güncelle, yoksa ekle (`ON CONFLICT DO UPDATE`) |
| z-skor | (değer − ortalama) / sapma; "kaç sapma uzakta" |
| rolling(N) | pandas'ta kayan pencere: her satır için son N satır |
