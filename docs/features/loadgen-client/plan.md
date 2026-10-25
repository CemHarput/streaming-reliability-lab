# Plan: loadgen-client

Doğrulanmış olgular (HEAD 24d617c; kod doğrudan okundu, code-graph MCP kullanılmadı):

- **Build:** Maven 3.9.16 (`.mvn/wrapper/maven-wrapper.properties`). Root `pom.xml`: `java.version=26`, `maven.compiler.release`. Preview flag yok, enforcer yok. `dependencyManagement` içinde grpc-bom, protobuf, `spring-boot-dependencies` 4.1.1 (import), `HdrHistogram` 2.2.2 ve `proto` var. `pluginManagement` içinde compiler 3.14.1, surefire 3.5.4, `maven-jar-plugin` 3.4.2, protobuf ve spring-boot-maven-plugin var. **`maven-dependency-plugin` ile `exec-maven-plugin` yönetilmiyor.** JaCoCo 0.8.15 yalnızca `prepare-agent` ve `report` çalıştırıyor; `check` goal'u yok, bu yüzden **coverage eşiği de yok**. Modül sırası proto, simulator, gateway, loadgen. CI `./mvnw -B verify` koşuyor (ubuntu, temurin 26, 15 dk).
- **`loadgen/pom.xml`:** yalnızca `HdrHistogram` bağımlılığı var. Açıklaması "Closed-model ... and open-model ..." diyor. `src/main/java/.../loadgen/package-info.java` dışında kaynak yok, test yok. Javadoc açık modeli ve "intended start time" kaydını vaat ediyor.
- **Test bağımlılığı emsali:** `simulator/pom.xml` `junit-jupiter`, `assertj-core` ve `awaitility`'yi sürümsüz alıyor (sürümler Boot BOM'undan geliyor). `exec-maven-plugin` 3.6.3'ü sürümüyle ve `mainClass` ile tanımlıyor.
- **Gateway endpoint'i** (`StreamController`): `GET /stream`, `produces=text/event-stream`. Parametreler: `chunks`=10, `firstDelayMs`=0, `intervalMs`=50, `fault`=NONE (`FaultParam`: NONE/UNAVAILABLE/INTERNAL/HANG), `faultAtSeq`=0, `seed`=0. Davranışlar:
  - Negatif `faultAtSeq`, bilinmeyen fault adı veya sayı olmayan değer HTTP 400 döner.
  - `chunks=0` gibi anlamsal hatalar 200 ve `error` `INVALID_ARGUMENT` olarak döner (`StreamEndToEndTest`).
  - Ret durumunda yanıt `503` ve `Retry-After: 1` olur. Gövde yok, ret sebebi yanıtta görünmüyor.
  - 200 yanıtında `X-Request-Id` (UUID) header'ı var.
- **SSE wire formatı** (`StreamBridge` + `SseEvents`):
  - `chunk`: `id` = seq, data `{"requestId","seq","text","last"}`.
  - `complete`: data `{"requestId","chunks"}`, `id` yok.
  - `error`: data `{"requestId","code","message"}`; `code` gRPC status adıdır.
  - Terminal olaydan sonra `emitter.complete()` çağrılıyor.
  - Heartbeat/comment, `retry:` alanı ve `[DONE]` sentinel'i yok. JSON'u Spring/Jackson tek satır üretiyor.
- **`application.yaml`:** `server.port` tanımlı değil, yani 8080 varsayımı. first-chunk ve idle timeout 30 s, stream-deadline 60 s, emitter timeout deadline + 5 s = 65 s. `max-active-streams=100`, `reject` politikası.
- **README:** gateway'in nasıl başlatılacağını yazmıyor (simulator için `exec:java` var). 73. satır "Modules without tests (`proto`, `loadgen` for now)" diyor.
- **Gateway test client'ı:** `gateway/src/test/.../testsupport/SseClient` JDK `HttpClient`'ı HTTP/1.1'e zorlayıp `BodyHandlers.ofLines()` kullanıyor; parser'ı basit (multi-line data yok, CR yok).
- **Test konvansiyonları:** `@Timeout(value=…, threadMode=SEPARATE_THREAD)` her yerde kullanılıyor; testlerde `Thread.sleep` yok.

## 1. Davranış ve kabul kriterleri

`loadgen`, gateway'e yalnızca HTTP üzerinden konuşan, Spring içermeyen bir CLI'dır. **Yalnız kapalı model** uygular: N worker'ın her biri bir akış açar, akış bitince düşünme süresi olmadan bir sonrakini açar. Worker'lar `--ramp-up` boyunca lineer biçimde başlar. Her istek için JSONL'e tek bir ham kayıt yazılır. Dosya bir header kaydı ile başlar, bir footer kaydı ile biter. Stdout'a ve isteğe bağlı bir JSON dosyasına "convenience" etiketli bir özet yazılır.

**Açık model (arrival rate) bu feature'da ertelenir.** Gerekçe: açık modelin doğru yapılması gerekenler planlanmış başlangıç zamanından gecikme ölçümü, generator geride kaldığında backlog politikası ve sınırsız eşzamanlılık koruması. Bunlar ayrı bir tasarım. Kayıt şemasında `scheduledOffsetNanos` alanı şimdiden var, böylece açık model şemayı değiştirmeden eklenebilir.

**Coordinated omission:** kapalı modelde gecikme, isteğin fiilen gönderildiği andan ölçülür. Sunucu yavaşladıkça sunulan yük de düşer; kuyrukta bekleyecek "olmayan" istekler ölçülmez. Bu durum header'da (`loadModel`, `coordinatedOmission` alanları), stdout özetinde ve README'de açıkça yazılır. Düzeltme yapılmaz. `scheduledOffsetNanos` ile `startOffsetNanos` arasındaki fark yalnızca generator'ın kendi zamanlama gecikmesini gösterir.

- **AC1 (build/bağımlılık):**
  - `./mvnw -B verify` art arda 3 kez yeşil geçer.
  - `loadgen` runtime bağımlılığı yalnızca `HdrHistogram`. Test kapsamında `junit-jupiter`, `assertj-core` ve `awaitility` var.
  - `loadgen` altında `io.github.cemharput.streamlab.gateway|simulator|tokenstream` import'u yoktur. `gateway`, `simulator` ve `proto` dizinlerinde diff boştur.
- **AC2 (SSE parser):** artımlı bir byte parser'ı.
  - Okumalar arasında bölünmüş satırları ve olayları (`\r\n`'nin `\r` | `\n` olarak bölünmesi dahil), bölünmüş UTF-8 çok-byte karakterlerini, LF/CRLF/CR satır sonlarını, baştaki BOM'u ve `:` ile başlayan comment'leri doğru işler.
  - `event`/`id`/`data` alanlarını ve çok satırlı `data`'yı (`\n` ile birleştirilir) destekler. İki noktası olmayan satırda alan adı satırın kendisi, değer boştur. Bilinmeyen alanlar yok sayılır. Değerdeki tek baştaki boşluk atılır.
  - Boş satır olayı dispatch eder. `data` boşsa dispatch olmaz (WHATWG). Akış sonunda yarım kalan olay atılır.
  - `maxLineBytes` (1 MiB) aşılırsa `SseProtocolException` fırlatılır.
- **AC3 (istek sonucu sınıflandırması):** her istekten tam olarak bir `outcome` çıkar:
  - `COMPLETED`: 200 ve `complete` olayı alındı.
  - `ERROR_EVENT`: 200 ve `error` olayı alındı; `errorCode` ve `errorMessage` dolu.
  - `REJECTED`: 503; `retryAfter` header'ın ham değeri.
  - `HTTP_ERROR`: diğer 200 dışı status'ler (ör. 400).
  - `PROTOCOL_ERROR`: 200 ama `Content-Type` `text/event-stream` değil, ya da parser limiti aşıldı.
  - `NO_TERMINAL`: gövde temiz EOF ile bitti ama terminal olay gelmedi.
  - `RESET`: header'lardan sonra, terminal olaydan önce I/O hatası (RST, yarım kalan chunked gövde).
  - `TIMEOUT`: `timeoutKind` = `connect`, `headers` veya `idle`.
  - `CONNECT_FAILURE`: ör. `ConnectException`.
  - `INTERRUPTED`: drain süresi doldu ya da sinyal geldi.
  - `CLIENT_ERROR`: beklenmeyen her şey; `errorClass` dolu.

  Terminal olaydan sonra gövdenin nasıl bittiği `bodyEnd` alanına yazılır ve outcome'u değiştirmez. Kontrat sapmaları `protocolViolations` alanında sayılır: terminalden sonra gelen olay, ikinci terminal, bilinmeyen olay adı, sayı olmayan ya da ardışık olmayan chunk `id`'si, `code` alanı olmayan `error`.
- **AC4 (ölçümler):**
  - Tüm süreler ve offset'ler `System.nanoTime` tabanlı tamsayı nanosaniyedir.
  - Kayıttaki alanlar: `ttfbNanos` (yanıt header'ları alındı), `firstBodyByteNanos`, `ttfcNanos` (ilk `chunk` olayı), `totalNanos` (başlangıçtan outcome anına), `interChunkMinNanos`/`interChunkMaxNanos` (en az 2 chunk varsa), `chunks`, `lastSeq`.
  - Değerler arasında `ttfb ≤ firstBodyByte ≤ ttfc ≤ total` sırası tutar.
  - Run başına inter-chunk histogramı footer'a sıkıştırılmış biçimde yazılır.
- **AC5 (yük modeli):**
  - En fazla `--concurrency` akış aynı anda açık olur; test sunucusunda ölçülür.
  - Worker `w`'nin ilk planlanan offset'i `rampUp * w / concurrency` olur (tam sayı aritmetiği). Her kayıtta `startOffsetNanos ≥ scheduledOffsetNanos`.
  - `--requests M` tam M kayıt üretir, index'ler `0..M-1` ve tekil.
  - `--duration` bittikten sonra yeni istek başlamaz. Açık akışlar `--drain-timeout` kadar beklenir. Kalanlar `INTERRUPTED` olarak kısmi verileriyle kaydedilir. Run sınırlı süre içinde döner.
- **AC6 (determinizm):** istek parametreleri yalnızca `(seed, index)` fonksiyonudur ve kendi SplitMix64 implementasyonuyla üretilir. Aynı seed aynı diziyi verir; sabit golden değerler testte pinlenir. Zamanlama, worker-index eşlemesi, outcome'lar ve gateway'in `requestId`'si deterministik değildir; README'de yazılır.
- **AC7 (çıktı):**
  - JSONL'in ilk satırı `type=header`, son satırı `type=footer`; aradakiler `type=request`. Bütün nesneler düz (iç içe değil).
  - JSON escaping RFC 8259'a uyar (`"`, `\`, `<0x20` için `\u00XX`) ve testlerle doğrulanır.
  - Stdout özeti ve `--summary-output` JSON'u şunları içerir: outcome başına sayılar; `ttfc`, `total` ve inter-chunk için p50/p90/p99/p99.9/max/count (µs). Başlık "CONVENIENCE SUMMARY – not a benchmark result" etiketini taşır.
- **AC8 (CLI):**
  - Bölüm 2'deki seçenekler desteklenir. Geçersiz, eksik, tekrarlanan veya bilinmeyen argümanlar ile `--duration`/`--requests` için "ikisi birden" veya "hiçbiri" durumu exit 2 ile ve tek satırlık net bir mesajla sonlanır; bu durumda çıktı dosyası oluşturulmaz.
  - `--help` exit 0 döner. I/O ya da beklenmeyen hata exit 1 döner.
  - Run tamamlanırsa, outcome'lardan bağımsız olarak exit 0 döner.
- **AC9 (çalıştırma):**
  - `./mvnw -B -pl loadgen package -DskipTests` sonrası `java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar --help` çalışır. Jar manifest'inde `Main-Class` ve `Class-Path: lib/HdrHistogram-2.2.2.jar` bulunur, `target/lib` doludur.
  - Yerel gateway'e karşı manuel komut JSONL ve özet üretir. Bu README'de belgelenir, CI'da assert edilmez.
- **AC10 (dokümanlar):**
  - README'ye loadgen bölümü eklenir: kullanım, manuel komut, çıktı şeması, ölçülenler ve ölçülmeyenler (sunucu tarafı süre, GC, ağ, client'ın kendi darboğazı, bağlantı yeniden kullanımı), aynı makinede çalışma notu, coordinated omission notu, açık modelin ertelendiği ve "no performance claim" ifadesi.
  - Modül tablosu satırı, durum paragrafı ve JaCoCo satırı güncellenir.
  - `docs/adr/0004-loadgen-closed-model-raw-output.md` eklenir.
  - `loadgen/pom.xml` açıklaması ve `package-info.java` gerçeğe uyarlanır.
- **AC11 (hijyen):**
  - Testlerde `Thread.sleep` yok.
  - Her test sınıfında `@Timeout(threadMode = SEPARATE_THREAD)` var.
  - Beklemeler sınırlıdır ve en az 5 kat pay taşır.
  - Sunucular, gate'ler ve HttpClient `finally` veya `@AfterEach` içinde kapatılır.
  - Modül testlerinin toplamı yaklaşık 30 s'nin altında kalır.
- **AC12:** bir test ya da manuel kontrol gateway'de bir defekt bulursa iş durur, kanıtıyla raporlanır ve feature `blocked` olur. Gateway, simulator ve proto değiştirilmez. Assert zayıflatılmaz.

## 2. Dosyalar, semboller ve tasarım

### Client yığını: JDK `java.net.http.HttpClient` ve özel `BodySubscriber`

- Run başına tek bir `HttpClient`:
  - `version(HTTP_1_1)`: her açık akış bir TCP bağlantısı olur. HTTP/2 multiplexing bağlantı maliyetini gizlemez, h2c upgrade denenmez.
  - `connectTimeout(--connect-timeout)`.
  - `executor(Executors.newVirtualThreadPerTaskExecutor())`: varsayılan cached platform pool binlerce akışta platform thread şişirir.
  - Run sonunda `shutdownNow()` + `awaitTermination(5s)` çağrılır (`HttpClient` Java 21+ ile `AutoCloseable`).
- **Neden `ofInputStream`/`ofLines` değil:**
  - İkisi de header'lar gelince döner. Okuma bloklayıcıdır; başka bir thread'den `close()` ile bloklu okumanın açılması garanti değil.
  - Satır bazlı API CR/CRLF bölünmesini ve byte zamanlamasını gizler.
- **Özel `BodyHandler`/`BodySubscriber` (push tabanlı):**
  - `apply(ResponseInfo)` anı `ttfbNanos` olarak alınır. Bu ham ilk byte değil, HttpClient'ın header bloğunu tamamen parse ettiği andır. İçinde DNS/connect (bağlantı havuzdan gelmiyorsa), isteğin yazılması ve gateway'in header'ları commit etmesi (admission sonrası) vardır. Bu sınır README'de yazılır.
  - Boş olmayan ilk `onNext` anı `firstBodyByteNanos` olur.
  - Olay zamanı, olayı tamamlayan boş satırı taşıyan buffer'ın `onNext` anıdır. Aynı buffer'daki birden çok chunk aralığı 0 ns görünür; çözünürlük buffer gelişidir ve bu da belgelenir.
  - Parse işlemi `onNext` içinde, non-blocking yapılır. `request(1)` akışı kullanılır.
- **Timeout ve iptal:** worker virtual thread'i sonuç future'ını döngü içinde `get(idleTimeout - (now - lastProgress))` ile bekler. `lastProgress`, header'larda ve her `onNext`'te güncellenir. Süre aşılırsa `timeoutKind` header öncesinde `headers`, sonrasında `idle` olur. Ardından `subscription.cancel()` ve `responseFuture.cancel(true)` çağrılır. Bloklu okuma olmadığı için JDK'nın okuma açma davranışına bağımlılık yoktur. `HttpConnectTimeoutException` `connect` olarak sınıflanır. `get` sırasında `InterruptedException` drain veya sinyal anlamına gelir: önce iptal yapılır, outcome `INTERRUPTED` olur.
- **Reset sınıflandırması:** header'lardan sonra `onError(IOException)` gelmesi, terminal olay alınmadıysa `RESET` olur (RST veya "EOF reached while reading" gibi yarım kalan chunked gövde). Temiz `onComplete` ile terminal olay yoksa `NO_TERMINAL` olur. Header'lardan önceki `IOException` için `ConnectException` → `CONNECT_FAILURE`, `HttpTimeoutException` → `TIMEOUT`, diğerleri `RESET` olur; `errorPhase` alanı `before_headers` veya `body` değerini alır.
- **Terminalden sonra:** terminal olay alındıktan sonra EOF beklenir, en fazla idle timeout kadar. Bağlantı keep-alive havuzuna geri dönebilsin diye cancel çağrılmaz. Sonuç `bodyEnd` alanına `eof`/`reset`/`timeout`/`interrupted` olarak yazılır.
- **200 dışı yanıtlar:** gövde en fazla 64 KiB'a kadar atılarak okunur, fazlası iptal edilir. `Retry-After` ham string olarak kaydedilir.
- **Eşzamanlılık sınırları** (README ve ADR'de yazılır):
  - Her client bir virtual thread'dir ve yük küçüktür.
  - JDK HTTP/1.1 havuzunda host başına sınır yoktur.
  - Pratik sınırlar şunlar: tek `SelectorManager` thread'i (client'ın kendisi darboğaz olabilir; ölçülmez), OS ephemeral port aralığı (Windows varsayılanı yaklaşık 16k, Linux yaklaşık 28k), timeout/iptal/reset sonrası client tarafı `TIME_WAIT`, Tomcat `max-connections` (varsayılan 8192, doğrulanmadı) ve gateway `max-active-streams` (100; üstü 503 alır).
  - Keep-alive havuzu yeniden kullanıldığı için kapalı modelde normalde port sayısı yaklaşık concurrency kadar olur.
  - CLI koruması: `--concurrency` 1..10000.
  - JDK, idempotent GET için havuzdan gelen eskimiş bir bağlantıda header öncesinde bir kez görünmez retry yapabilir. Süre ölçüme dahil olur; bu bir risk olarak belgelenir.

### SSE parser (`sse` paketi)

- `SseParser`: `feed(ByteBuffer buf, long nanos)` ve `end()`.
  - Byte satır buffer'ı (`byte[]`, büyüyen) ve `pendingCr` bayrağı tutar.
  - Satır `\n`, `\r` veya `\r\n`'de biter. UTF-8 çözme her tam satırda bir kez yapılır; CR ve LF ASCII olduğu için çok-byte karakter bölünmesi güvenlidir.
  - İlk satırdaki BOM atılır. Alan kuralları AC2'de.
  - Dinleyici: `SseListener.onEvent(SseEvent event, long nanos)`.
- `SseEvent(String type, String id, String data)`. `type` yoksa `"message"` olur.
- `SseProtocolException`.

### JSON (`json` paketi; Jackson eklenmez)

Jackson Boot BOM'unda yönetiliyor, ama runtime'a ağır bir bağımlılık eklemek isteğin "minimal" kısıtına ters. Kayıtlar düz olduğu için küçük, testli bir yazıcı ve okuyucu yeter.

- `JsonLine`: sıralı bir builder.
  - Metotlar: `field(String, String|long|Long|boolean|double)`, null değer, `toString()`.
  - Escaping: `"`, `\`, `\b\f\n\r\t`, diğer `<0x20` karakterler `\u00XX` olur. NaN ve Infinity reddedilir.
- `FlatJson.parse(String)`: düz bir nesneyi `Map<String,Object>` olarak döner (string/long/double/boolean/null; `\uXXXX` dahil escape'ler).
  - SSE `data`'sından `code`, `message` ve `chunks` okumak için kullanılır. Chunk seq SSE `id`'sinden alınır, `text` parse edilmez.
  - Testler JSONL çıktısını da bununla okur.

### Client (`client` paketi)

- `StreamParams(int chunks, int intervalMs, int firstDelayMs, String fault, int faultAtSeq, long seed)`. `toQuery()` tüm parametreleri açıkça yazar, gateway varsayılanlarına güvenilmez.
- `StreamClient(HttpClient, URI target, Duration idleTimeout)`. `execute(StreamParams, long runStartNanos)` sonucu `StreamResult` olarak döner; interrupt'a duyarlıdır.
- `StreamSubscriber` (`BodySubscriber<Void>`): ölçümleri toplar ve iç inter-chunk aralıklarını run'ın `ConcurrentHistogram`'ına kaydeder.
- `StreamResult` (record) ve `Outcome` (enum, AC3).

### Run (kök paket `io.github.cemharput.streamlab.loadgen`)

- `LoadgenMain`:
  - `main(String[])` → `System.exit(run(args, System.out, System.err))`.
  - `static int run(String[], PrintStream, PrintStream)` test edilebilir; içinde `System.exit` yoktur.
  - Shutdown hook `LoadRun.stop(SIGNAL)` çağırır ve en fazla `drainTimeout + 5 s` bekler. Run normal biterse hook no-op'tur.
- `Options` (record) ve `OptionsParser` (`UsageException` fırlatır).
  - Elle yazılmıştır; `--name value` ve `--name=value` biçimlerini kabul eder.
  - Süreler `<int>ms|s|m` biçimindedir. Aralıklar `N` veya `MIN-MAX` (kapalı aralık) biçimindedir.
- `RequestVariation`:
  - SplitMix64 kullanır: `state = seed + index * 0x9E3779B97F4A7C15L`.
  - Sabit çekiliş sırası: chunks, intervalMs, firstDelayMs, faultApplies (`< faultRate`), faultAtSeq, contentSeed.
  - Aralık çekilişi `Math.floorMod`; modulo bias'ı küçük aralıklarda ihmal edilebilir ve belgelenir.
  - `faultAtSeq ≥ chunks` olursa fault hiç tetiklenmez; bu durum pass-through kalır ve belgelenir.
- `LoadRun`:
  - Global `AtomicLong nextIndex` tutar; `--requests` modunda limit uygulanır.
  - `CountDownLatch stopSignal` vardır. Ramp beklemesi ve reject sonrası duraklama `stopSignal.await(remaining)` ile yapılır; ikisi de stop gelince erken uyanır. `Thread.sleep` yoktur.
  - Worker'lar `Thread.ofVirtual().name("loadgen-worker-", 0)` ile oluşturulur.
  - Duration sonu tek bir platform daemon zamanlayıcıyla sinyallenir (`ScheduledExecutorService`).
  - Drain: worker'lar `--drain-timeout` deadline'ına kadar `join` edilir, kalanlar interrupt edilir ve en fazla 5 s daha join edilir.
  - Her worker'ın ilk planlanan offset'i `rampUp*w/N`, sonrakiler önceki isteğin bitişi (reject sonrası duraklama eklenmiş halde) olur.
  - Rampa sırasında süre biterse başlamamış worker'lar hiç başlamaz.
- `RecordWriter`:
  - Tek platform writer thread'i, sınırsız bir `LinkedBlockingQueue` okur. Sınırlı kuyruk worker'ları bloklayıp zamanlamayı bozardı; bellek riski belgelenir.
  - UTF-8 `BufferedWriter` kullanır ve kuyruk boşaldığında, ayrıca kapanışta flush eder.
  - Header, worker'lar başlamadan önce senkron yazılır; footer en son yazılır.
  - Yazma hatasında run durur ve exit 1 döner.
  - Ttfc/total histogramlarını ve outcome sayaçlarını tek thread'de (writer) kaydeder.
- `Summary`:
  - `new Histogram(3)` (auto-resize, 3 anlamlı basamak, yaklaşık %0,1 göreli hassasiyet) ve ns değerleri kullanır; çıktı µs (`value/1000`).
  - İki biçimde yazılır: stdout metni ve `--summary-output` düz JSON (`type=summary`).

### Kayıt şeması (`schemaVersion=1`, alan sırası sabit, düz nesneler)

- **header:**
  - Kimlik ve zaman: `type`, `schemaVersion`, `tool="streamlab-loadgen"`, `toolVersion` (manifest `Implementation-Version`, yoksa `"unknown"`), `startedAt` (ISO-8601 UTC, wall clock).
  - Hedef ve model: `target`, `loadModel="closed"`.
  - Parametreler: `concurrency`, `durationMs|null`, `requests|null`, `rampUpMs`, `chunks`, `intervalMs`, `firstDelayMs`, `faultAtSeq` (aralık string'leri, ör. `"5-20"`), `fault`, `faultRate`, `seed`, `connectTimeoutMs`, `idleTimeoutMs`, `drainTimeoutMs`, `rejectPauseMs`.
  - Ortam: `httpVersion="HTTP/1.1"`, `javaVersion`, `javaVendor`, `javaVmName`, `osName`, `osArch`, `osVersion`, `availableProcessors`, `maxHeapBytes`.
  - Notlar: `timeUnit="ns"`, `clock="System.nanoTime; offsets relative to run start; client and gateway are expected on one machine"`, `coordinatedOmission="not corrected: closed model, latency from actual send, slower responses lower the offered load"`, `commandLine`.
- **request:**
  - Kimlik ve parametreler: `type`, `index`, `worker`, `paramChunks`, `paramIntervalMs`, `paramFirstDelayMs`, `paramFault`, `paramFaultAtSeq`, `paramSeed`.
  - Zaman: `scheduledOffsetNanos`, `startOffsetNanos`, `endOffsetNanos` (kaydın kapandığı an, gövde bitişi dahil).
  - Sonuç: `outcome`, `status|null`, `retryAfter|null`, `requestId|null`.
  - Ölçümler: `ttfbNanos|null`, `firstBodyByteNanos|null`, `ttfcNanos|null`, `totalNanos`, `chunks`, `lastSeq|null`, `interChunkMinNanos|null`, `interChunkMaxNanos|null`.
  - Terminal ve hata: `terminal` (`complete|error|none`), `errorCode|null`, `errorMessage|null` (512 karakter), `completeChunks|null`, `protocolViolations`, `timeoutKind|null`, `errorPhase|null`, `errorClass|null`, `errorDetail|null` (512 karakter), `bodyEnd|null`.
- **footer:** `type`, `endedAt`, `elapsedNanos`, `stopReason` (`duration|requests|signal`), `requestsStarted`, `recordsWritten`, `interrupted`, `interChunkCount`, `interChunkHistogramUnit="ns"`, `interChunkHistogram` (HdrHistogram `encodeIntoCompressedByteBuffer` + Base64).

### CLI seçenekleri

| Seçenek | Varsayılan | Doğrulama |
|---|---|---|
| `--target URL` | zorunlu | `http`/`https`, host var, query yok (ör. `http://localhost:8080/stream`) |
| `--concurrency N` | 1 | 1..10000 |
| `--duration D` / `--requests M` | tam olarak biri zorunlu | D > 0; M 1..100000000 |
| `--ramp-up D` | 0s | ≥ 0; `--duration` ile birlikte < duration |
| `--chunks N\|A-B` | 10 | 1..1000000, A ≤ B |
| `--interval-ms`, `--first-delay-ms` | 50, 0 | 0..60000 (simulator `RequestLimits.DEFAULT`'u yansıtır; kayma riski not edilir) |
| `--fault` | NONE | NONE/UNAVAILABLE/INTERNAL/HANG |
| `--fault-at-seq N\|A-B` | 0 | ≥ 0 |
| `--fault-rate P` | 1.0 | 0..1 |
| `--seed L` | 0 | long |
| `--output PATH` | zorunlu | üst dizin mevcut; dosya varsa `--overwrite` yoksa hata |
| `--summary-output PATH` | yok | aynı kural |
| `--connect-timeout D` | 5s | > 0 |
| `--idle-timeout D` | 90s | > 0 (gateway'in 30 s timeout'larından ve 65 s emitter timeout'undan büyük) |
| `--drain-timeout D` | 30s | ≥ 0 |
| `--reject-pause D` | 100ms | ≥ 0 (503 sonrası düşünme süresi; açık soru S1) |
| `--overwrite`, `--help`/`-h` | – | bayrak |

### Pom

- **Root `pom.xml`:** `<maven-dependency-plugin.version>3.8.1</maven-dependency-plugin.version>` property'si ve pluginManagement girdisi eklenir. Sürümün Central'da bulunduğu ilk build'de doğrulanmalı (varsayım A1).
- **`loadgen/pom.xml`:**
  - Açıklama "Closed-model load generator client for the gateway's SSE endpoint (raw JSONL output)" olur.
  - Test bağımlılıkları eklenir: `junit-jupiter`, `assertj-core`, `awaitility`; hepsi sürümsüz ve test kapsamında, simulator'daki gibi.
  - `maven-jar-plugin` için `archive.manifest`: `mainClass=io.github.cemharput.streamlab.loadgen.LoadgenMain`, `addClasspath=true`, `classpathPrefix=lib/`, `addDefaultImplementationEntries=true`.
  - `maven-dependency-plugin`: `copy-dependencies` goal'u `package` fazında çalışır, `includeScope=runtime`, `outputDirectory=${project.build.directory}/lib`.
  - Shade, assembly, exec ve Spring eklenmez. `exec:java` reddedildi, çünkü ölçüm Maven JVM'inde yapılırdı ve `System.exit` Maven'ı öldürürdü.

### E2E smoke testi: bilinçli olarak dışarıda

- Gateway'in ana artefaktı `spring-boot-maven-plugin repackage` ile fat jar'a dönüşüyor (sınıflar `BOOT-INF/classes` altında), bu yüzden bağımlılık olarak kullanılamaz.
- `InProcessSimulatorConfig` gateway'in test kaynaklarında; test-jar gerekirdi.
- İstek gateway main koduna bağımlılığı yasaklıyor.

Bu nedenle uçtan uca kontrol README'deki manuel komutla yapılır. Script düzeyinde bir smoke testi `benchmark-protocol-run` feature'ına bırakılır.

### Gateway bulguları (değişiklik yapılmaz, rapor edilir)

- **F1:** README gateway'in nasıl başlatılacağını yazmıyor. Varsayım: `java -jar gateway/target/gateway-0.1.0-SNAPSHOT.jar`, port 8080 (manuel kontrolde doğrulanacak). Loadgen bölümündeki komut bu varsayımla yazılır.
- **F2:** 503 yanıtında sebep yok (`limit`/`queue_timeout`/`shutdown`/`upstream_unavailable` ayırt edilemiyor); loadgen yalnızca status ve `Retry-After` görür. Sebep yalnızca `rejected_total{reason}` metriğinden alınabilir.
- **F3:** Anlamsal parametre hataları 400 değil, 200 ve `error` `INVALID_ARGUMENT` olarak dönüyor. Loadgen bu yüzden aralıkları kendisi doğrular.
- **F4:** SSE heartbeat yok. Client idle timeout'u, sunucunun 30 s first-chunk ve idle timeout'larından büyük tutulmalı.

## 3. Uygulama adımları

1. Pom değişiklikleri (root property ve pluginManagement, `loadgen/pom.xml`). `./mvnw -B -pl loadgen package -DskipTests` sonrası manifest ve `target/lib` kontrol edilir.
2. `json/JsonLine`, `json/FlatJson` ve `JsonLineTest`, `FlatJsonTest` (escape'ler, control karakterleri, unicode, null, round-trip, hatalı giriş).
3. `sse/SseParser`, `SseEvent`, `SseProtocolException` ve `SseParserTest`.
   - Her byte sınırında bölme sweep'i: aynı girdi tüm split noktalarında aynı olayları vermeli.
   - CRLF/CR/LF ve `\r|\n` bölünmesi, bölünmüş UTF-8, BOM.
   - Comment, multi-line data, iki noktasız satır, bilinmeyen alan, boş data'nın dispatch edilmemesi, sonda yarım kalan olay, uzun satır limiti.
   - Gateway formatında (`event:chunk\nid:0\ndata:{...}\n\n`) chunk, complete ve error olayları.
4. `OptionsParser`, `Options` ve `OptionsParserTest`.
   - Tüm seçenekler, `=` biçimi.
   - Hatalar: eksik değer, bilinmeyen seçenek, tekrar, duration ile requests birlikte veya hiçbiri, aralık ve sınır ihlalleri, ramp ≥ duration, var olan çıktı dosyası.
   - Help metni.
5. `RequestVariation` ve `RequestVariationTest`: golden değerler (seed 42, index 0..2), aynı seed aynı sonuç, farklı seed farklı sonuç, sınırlar içinde kalma, `faultRate` 0 ve 1 uçları, tek değerli aralıklar.
6. `client/*` ve `StreamClientTest`.
   - `com.sun.net.httpserver.HttpServer` `127.0.0.1:0` üzerinde, virtual-thread executor ile çalışır. Handler `sendResponseHeaders(200, 0)` ile chunked yanıt verir. Byte'lar test kontrollü `CountDownLatch` gate'lerle parça parça yazılır ve flush edilir.
   - Reset testleri için ayrı bir `RawSocketServer` fixture'ı (test kodu): `ServerSocket` isteği okur, elle bir HTTP/1.1 chunked yanıtı yazar, sonra `setSoLinger(true, 0)` ve `close()` ile RST gönderir. Bir varyantı düz `close()` ile chunked gövdeyi yarım bırakır, bir diğeri `Connection: close` ile gövdesiz uzunluk verip temiz EOF yapar.
   - Senaryolar ve beklenenler:
     - normal → `COMPLETED`, `chunks`/`lastSeq`/`completeChunks` doğru, süre sırası tutar
     - `error` olayı → `ERROR_EVENT`, kod ve mesaj
     - 503 + `Retry-After: 1` → `REJECTED`, `retryAfter="1"`, ttfc null
     - 400 → `HTTP_ERROR`
     - yanlış content-type → `PROTOCOL_ERROR`
     - RST → `RESET`, chunks=1, `errorPhase=body`
     - yarım chunked → `RESET`
     - temiz EOF terminalsiz → `NO_TERMINAL`
     - terminalden sonra olay → `protocolViolations>0` ve outcome değişmez
     - kapalı port → `CONNECT_FAILURE`
     - header öncesi takılma (idle 300 ms) → `TIMEOUT/headers`
     - bir chunk sonra takılma → `TIMEOUT/idle`, chunks=1
   - Her testte sunucu `stop(0)` ile durdurulur, gate'ler `@AfterEach` içinde açılır, `HttpClient.shutdownNow()` çağrılır.
7. `RecordWriter`, `Summary`, `LoadRun` ve `LoadRunTest`.
   - concurrency 8 + requests 50: 50 tekil index, worker 0..7, sunucunun ölçtüğü en yüksek eşzamanlılık ≤ 8, sayılar toplanır.
   - Ramp: 400 ms ve 4 worker için ilk planlanan offset'ler tam 0/100/200/300 ms; `start ≥ scheduled`.
   - Duration + drain başarılı: takılan akışlar, `isStopping()` Awaitility ile görülünce gate açılır; kayıtlar `COMPLETED`, sayı concurrency kadar, stop'tan sonra yeni istek yok.
   - Duration + drain süresi doluyor (drain 300 ms): kayıtlar `INTERRUPTED`, chunks=1, footer `interrupted` sayısı doğru, run sınırlı sürede döner.
   - 503 sonrası `--reject-pause` stop sinyaliyle erken uyanır.
   - Footer histogramı decode edilip `interChunkCount` ile karşılaştırılır.
8. `LoadgenMain` ve `LoadgenMainTest`.
   - `@TempDir` ve HttpServer ile uçtan uca CLI: exit 0; JSONL'in ilk satırı header, son satırı footer, N request kaydı; summary JSON'unda sayılar; stdout'ta "CONVENIENCE SUMMARY".
   - Geçersiz argüman exit 2 döner, stderr'e mesaj yazılır, dosya oluşmaz. `--help` exit 0 döner.
9. Dokümanlar:
   - `package-info.java` ve pom açıklaması.
   - `README.md`: durum paragrafı, modül satırı ("closed model + linear ramp, raw JSONL + convenience summary; open model deferred"), JaCoCo satırı, yeni "Load generator" bölümü (AC10 içeriği, şema tabloları, örnek komut) ve Decisions listesine ADR-0004.
   - `docs/adr/0004-loadgen-closed-model-raw-output.md`: kapalı model + ramp, açık modelin ertelenmesi, coordinated omission'ın açıklanması, HTTP/1.1 kararı, push subscriber, elle yazılmış JSON, jar + `lib/`, E2E testin neden olmadığı.
10. Odaklı komutlar, ardından 3 kez tam `verify`, ardından manuel kontrol.

Yeni main dosyaları (`loadgen/src/main/java/io/github/cemharput/streamlab/loadgen/`): `LoadgenMain`, `Options`, `OptionsParser`, `UsageException`, `RequestVariation`, `LoadRun`, `RecordWriter`, `Summary`, `sse/SseParser`, `sse/SseEvent`, `sse/SseListener`, `sse/SseProtocolException`, `json/JsonLine`, `json/FlatJson`, `client/StreamClient`, `client/StreamSubscriber`, `client/StreamParams`, `client/StreamResult`, `client/Outcome`.

Testler (`loadgen/src/test/java/...`): yukarıdaki test sınıfları, ayrıca `testsupport/SseTestServer` (HttpServer sarmalayıcı) ve `testsupport/RawSocketServer`.

## 4. Doğrulama

| AC | Kontrol | Beklenen |
|---|---|---|
| AC1 | 3× `./mvnw -B verify`; `./mvnw -B -pl loadgen dependency:tree`; `loadgen` altında `streamlab.gateway\|simulator\|tokenstream` için grep; `git diff --stat -- proto simulator gateway` | exit 0; runtime'da yalnızca HdrHistogram; eşleşme yok; diff boş |
| AC2 | `SseParserTest` | tüm split noktalarında aynı olaylar; limit aşımı exception |
| AC3 | `StreamClientTest` | her senaryo beklenen `outcome`/`timeoutKind`/`errorPhase` |
| AC4 | `StreamClientTest`, `LoadRunTest` | null olmama ve süre sırası; footer histogramı decode edilir |
| AC5 | `LoadRunTest` | eşzamanlılık ≤ N, ramp offset'leri tam, drain davranışı |
| AC6 | `RequestVariationTest` | golden değerler eşleşir |
| AC7 | `JsonLineTest`, `FlatJsonTest`, `LoadgenMainTest` | escaping doğru; header/footer yerinde; etiketli özet |
| AC8 | `OptionsParserTest`, `LoadgenMainTest` | exit 2/0/1; hata durumunda dosya yok |
| AC9 | `package` sonrası `java -jar ... --help`; manuel run | exit 0; JSONL + özet |
| AC10 | doküman incelemesi | performans iddiası yok; ölçülmeyenler listelenmiş |
| AC11 | kaynak incelemesi; surefire süreleri | `Thread.sleep` yok; yaklaşık 30 s altı |

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- `./mvnw -B -pl loadgen -am verify -Dtest=SseParserTest,JsonLineTest,FlatJsonTest,OptionsParserTest,RequestVariationTest,StreamClientTest,LoadRunTest,LoadgenMainTest -Dsurefire.failIfNoSpecifiedTests=false` (loadgen yalnızca parent'a bağlı olduğundan `-am` zararsız)
- `./mvnw -B verify` (3 kez)
- Manuel kontrol (README'ye yazılır, CI'da çalışmaz):
  1. `./mvnw -q -pl simulator -am install -DskipTests` ve `./mvnw -q -pl simulator exec:java`
  2. `./mvnw -B -pl gateway -am package -DskipTests` ve `java -jar gateway/target/gateway-0.1.0-SNAPSHOT.jar`
  3. `./mvnw -B -pl loadgen package -DskipTests`
  4. `java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar --target http://localhost:8080/stream --concurrency 10 --duration 30s --ramp-up 5s --chunks 20 --interval-ms 20 --seed 42 --output results/run.jsonl --summary-output results/summary.json`

**Riskler:**
- **Zamanlamaya bağlı flakiness:** testler süre değeri assert etmez; yalnızca sıra, null olup olmama ve sayılar kontrol edilir. Timeout testleri küçük idle değerleri (300 ms) ile `atMost(10s)` beklemeleri kullanır. Ramp kontrolü planlanan offset'in hesaplanan değeri üzerinden yapılır, ölçülen değer üzerinden değil.
- **JDK HttpClient davranışları:**
  - Yarım kalan chunked gövdenin `onError` ile gelmesi varsayıldı. Test ilk koşuda doğrular; tutmazsa sınıflandırma `NO_TERMINAL` olarak düzeltilir ve belgelenir.
  - `cancel(true)` ile bağlantının kapanması varsayıldı.
  - Header öncesinde görünmez bir retry olabilir.
- **Windows'ta RST:** `SO_LINGER 0` ile RST'nin gönderileceği varsayıldı; test bunu doğrular.
- **Port tükenmesi ve `TIME_WAIT`:** testlerde concurrency ≤ 8 tutulur; gerçek kullanım için README'de yazılır.
- **HTTP/1.1 zorunluluğu:** bağlantı yeniden kullanımı kayıtlarda görünmez (bu API ile ölçülemiyor) ve ttfb'ye etkisi değişkendir.
- **Zaman kaynakları:** wall clock yalnızca `startedAt`/`endedAt` için kullanılır; ölçümler `nanoTime` ile yapılır.
- **Coordinated omission:** düzeltilmez, açıklanır.
- **Writer kuyruğu:** sınırsız olduğu için çok yüksek oranlarda bellek büyüyebilir.
- **Bağımlılık sürümü:** `maven-dependency-plugin` 3.8.1 Central'da bulunamazsa build kırılır (A1).

**Varsayımlar:**
- **A1:** `maven-dependency-plugin` 3.8.1 mevcut.
- **A2:** Spring `SseEmitter` header'ları akış başında commit ediyor, yani ttfb yaklaşık admission anına denk geliyor. Satır sonları LF; parser her durumda toleranslı.
- **A3:** gateway portu 8080 ve `java -jar` ile çalışıyor (F1).
- **A4:** `com.sun.net.httpserver` classpath'te kullanılabiliyor (standart JDK).
- **A5:** Boot BOM `junit-jupiter`, `assertj-core` ve `awaitility` sürümlerini yönetiyor (simulator'da doğrulandı).

**Defekt durumunda:** testler veya manuel kontrol gateway kontratına aykırı bir davranış gösterirse (ör. terminal sonrası olay, ardışık olmayan id, header'ların geç commit edilmesi) iş durur. Kanıt (kayıt satırı, komut) ile rapor edilir, feature `blocked` olur. Gateway değiştirilmez, assert zayıflatılmaz.

**Açık sorular** (bloklayıcı değil; varsayılan kararlar alındı):
- **S1:** 503 sonrası worker `--reject-pause` (varsayılan 100 ms) kadar bekleyip tekrar dener. Bu, kapalı modele bir düşünme süresi ekler, ama aksi halde concurrency > `max-active-streams` olduğunda 503 hot-loop'u oluşur ve JSONL şişer. Alternatif: `Retry-After`'a uymak, ya da varsayılanı 0 yapmak.
- **S2:** Hiç `COMPLETED` olmasa bile run tamamlandıysa exit 0 döner; outcome'lar veri olarak ele alınır. Script'ler özetten kontrol eder. İstenirse `--fail-if-no-completed` eklenebilir.
- **S3:** Çıktı dosyası varsa `--overwrite` olmadan hata verilir.
