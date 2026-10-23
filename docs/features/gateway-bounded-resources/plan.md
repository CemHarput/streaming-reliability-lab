# Plan: gateway-bounded-resources

## 1. Davranış ve kabul kriterleri

Gateway'e üç sınır eklenir. Birincisi global aktif akış sınırıdır: aşılınca istek reddedilir ya da süreli olarak bekletilir. İkincisi istek başına sınırlı chunk kuyruğu ve upstream'de manuel akış kontrolüdür. Üçüncüsü Micrometer metrikleridir. `ActiveStreamRegistry` kabulün tek sahibi olarak kalır. `StreamBridge` tek terminal sonuç tasarımını (CAS claim, `sendLock`, `tryLock(5s)`) korur. Shutdown sırası ve süre sınırları değişmez. `proto/` ve `simulator/` dosyalarına dokunulmaz.

Terimler:
- **Permit:** Kapasiteden düşülen hak. Kabulle alınır, terminal sonuç claim edildiği anda bırakılır. Bunu terminal sonucu claim eden bırakır; son yazımı beklemez.
- **Tracked:** Stream registry'nin izlediği akışlar kümesindedir. Shutdown'ın boşaltma beklemesi ve zorla iptali bu kümeye bakar. Akış bu kümeden terminal olay yazımı döndükten sonra çıkar. Bugünkü davranış budur.

Kabul kriterleri:
- **AC1 (reject politikası):** `lab.limits.overload-policy=reject` ve `max-active-streams=N` ile N akış permit tutarken gelen yeni `GET /stream` hemen 503 alır. Yanıtta `Retry-After: 1` vardır, SSE açılmaz ve upstream çağrısı başlamaz (lease kapanır). `rejected_total{reason="limit"}` 1 artar. Bir permit boşalınca sonraki istek 200 ile normal akar.
- **AC2 (wait politikası):** `overload-policy=wait` ile dolu durumda gelen istek en fazla `lab.limits.queue-wait` kadar bekler. Bu süre içinde permit boşalırsa kabul edilir ve 200 ile normal akar. Süre dolarsa 503 + `Retry-After` döner, `rejected_total{reason="queue_timeout"}` artar ve geçen süre `queue-wait` değerinden kısa olmaz. Aynı anda bekleyen istek sayısı `lab.limits.max-waiting` ile sınırlıdır. Fazlası beklemeden 503 alır (`reason="limit"`).
- **AC3 (shutdown ve bekleyenler):** `ActiveStreamRegistry.stop()` başladığında bekleyen istekler hemen uyanır ve 503 alır (`reason="shutdown"`). Bu istekler `queue-wait` dolmadan, testte en geç 10s içinde yanıt alır. Bekleyenler `stop()` süresini uzatmaz: `close()` süresi mevcut sınır olan drain + 2s + kanal kapanma (5s) + 2s içinde kalır. `GracefulShutdownTest` değiştirilmeden yeşil kalır.
- **AC4 (sınırlı kuyruk ve manuel akış kontrolü):**
  - `beforeStart`, `disableAutoRequestWithInitial(streamQueueCapacity)` çağırır.
  - Her chunk istemciye başarıyla yazıldıktan sonra `request(1)` çağrılır. Son chunk'tan ve terminal sonuçtan sonra çağrılmaz.
  - Yavaş tüketicide istek başına kuyruktaki chunk sayısı hiçbir an `stream-queue-capacity` değerini aşmaz.
  - Upstream durur: fake upstream'in kredisi biter. In-process simulator'da `flow_control_pauses_total >= 1` olur ve `chunks_emitted_total <= capacity`.
  - Tüketici açılınca tüm chunk'lar sırayla gelir, ardından tek bir `complete` gelir.
- **AC5 (sıra ve tek terminal):**
  - Kuyruktaki chunk'lar upstream'in `error`/`complete` sonucundan önce yazılır.
  - Her akışta tam bir terminal olay olur ve ondan sonra hiçbir şey gönderilmez.
  - Shutdown iptali ya da istemci kopması kuyruktaki chunk'ları atar ve `queue_depth` değerini düşürür.
  - Upstream verilen krediden fazla chunk gönderirse kuyruk büyümez. Akış tek `error` (`RESOURCE_EXHAUSTED`) ile biter ve upstream iptal edilir.
- **AC6 (takılı terminal yazımı kapasite sızdırmaz):** Terminal sonucu claim edilen bir akış, permit'ini ve lease'ini son olay yazımından önce bırakır. Bu her claim için geçerlidir: final chunk ya da `complete` öncesi `COMPLETED`, `error` öncesi `UPSTREAM_ERROR`. Yazım takılı kalsa bile:
  - `admittedCount()` 0'a düşer.
  - `max-active-streams=1` iken yeni bir akış kabul edilir.
  - Lease tam bir kez kapanır.
  - Akış yazım dönene kadar tracked kalır (`activeCount()==1`). `stop()` bunu bugünkü gibi en fazla drain süresi kadar bekler ve tek WARNING loglar.
- **AC7 (metrikler):** `MeterRegistry` üzerinde şu metrikler bulunur:
  - Gauge `active_streams`: permit tutan akış sayısı.
  - Gauge `queue_depth{queue="chunks"}`: tüm akışların kuyruklarındaki chunk toplamı.
  - Gauge `queue_depth{queue="admission"}`: wait modunda bekleyen istek sayısı.
  - Counter `rejected_total{reason=limit|queue_timeout|shutdown|upstream_unavailable|interrupted}`.

  Testlerde değerler senaryoya uyar ve akışlar bitince gauge'lar 0'a döner. `/actuator/metrics/rejected_total` 200 döner.
- **AC8 (konfigürasyon):**
  - Varsayılanlar `@DefaultValue` ile gelir: `max-active-streams=100`, `overload-policy=reject`, `queue-wait=5s`, `max-waiting=100`, `stream-queue-capacity=16`.
  - Aşağıdaki geçersiz değerlerle uygulama başlamaz ve hata mesajı property adını içerir:
    - `max-active-streams<1`
    - `max-waiting<1`
    - `stream-queue-capacity` 1..4096 dışında
    - `queue-wait` sıfır ya da negatif
    - `overload-policy=drop`
- **AC9 (regresyon):**
  - Mevcut testler yeşil kalır. `StreamBridgeTest` asenkron yazıma uyarlanır ama iddiaları korunur.
  - `git diff --stat -- proto simulator` boş olur.
  - `gateway/src` altında `Thread.sleep` yoktur.
  - `./mvnw -B verify` yeşildir.
- **AC10 (dokümantasyon):**
  - README'deki ayar tablosu, metrikler ve "Known limits" güncellenir.
  - Slot sızıntısı maddesi yeni davranışa göre yeniden yazılır.
  - Takılı ara yazımda permit'in emitter timeout'a kadar tutulduğu açıkça yazılır ve doğrulanmamış hipotez olarak işaretlenir.

## 2. İlgili dosyalar, mevcut yaklaşım ve önerilen değişiklik

### Doğrulanmış gerçekler (kod ve `.m2` okundu)

**Mevcut gateway kodu:**
- `stream/StreamController.stream(...)` sırasıyla şunları yapar:
  1. `channels.acquire()` ile lease alır.
  2. `SseEmitter(streamDeadline+5s)` oluşturur.
  3. `new StreamBridge(requestId, emitter, registry, lease)` kurar.
  4. `registry.tryRegister(bridge)` çağırır; false gelirse `lease.close()` + `unavailable()` (503, `Retry-After: 1`) döner.
  5. `TokenStreamServiceGrpc.newStub(...).withDeadlineAfter(...).generateTokens(..., bridge)` ile çağrıyı başlatır; `RuntimeException` gelirse `bridge.onError(INTERNAL)` çağırır.
- `stream/StreamBridge`:
  - Terminal durum `AtomicReference<StreamOutcome>` ile tutulur: `COMPLETED`, `UPSTREAM_ERROR`, `CLIENT_GONE`, `SHUTDOWN_CANCELLED`.
  - `onNext` gRPC callback thread'inde `sendLock.lock()` altında doğrudan `emitter.send` yapar. Son chunk'ta önce `COMPLETED` claim edilir.
  - `release(outcome)` metodu `lease.close()` + `registry.unregister(this)` yapar. `COMPLETED` ve `UPSTREAM_ERROR` için yazımdan **sonra**, `CLIENT_GONE` ve `SHUTDOWN_CANCELLED` için claim anında çağrılır.
  - `sendErrorAndComplete` `tryLock(SEND_LOCK_WAIT=5s)` kullanır. `TERMINAL_EVENT_EXECUTOR` sanal thread'dir.
  - `beforeStart` yalnızca stream'i yayınlar; auto-request açıktır.
- `stream/ActiveStreamRegistry`:
  - `SmartLifecycle` olarak çalışır, faz `LifecyclePhases.STREAM_ADMISSION = DEFAULT_PHASE-512`.
  - Tek bir `ReentrantLock` ve `emptied` Condition kullanır; durum `Set<StreamBridge> active` ve `accepting` alanlarındadır.
  - `stop()` sırası: `accepting=false`, `awaitEmpty(drainTimeout)`, `cancelAll()` paralel çalışır, `FORCED_CANCEL_WAIT=2s` beklenir, sonra "still hold a slot" WARNING'i loglanır.
- `config/GatewayProperties`: `record(connectionMode, upstream, shutdown)`. Compact constructor doğrulama yapar, `requirePositive` vardır. Enum relaxed binding çalışır (`shared` → `SHARED`).
- `application.yaml`: virtual threads açık, `server.shutdown: graceful`, `timeout-per-shutdown-phase: 20s`, exposure `health,metrics,prometheus`.

**Bağımlılıklar:**
- `gateway/pom.xml` içinde Prometheus registry bağımlılığı **yok**. `prometheus` exposure şu an etkisizdir; dokunulmaz ve bağımlılık eklenmez.
- `spring-boot-starter-actuator-4.1.1.pom` şunları getirir: `spring-boot-starter-micrometer-metrics` → `spring-boot-micrometer-metrics`, `micrometer-observation:1.17.1`, `micrometer-jakarta9:1.17.1`.
- `.m2` içinde `micrometer-core-1.17.1.jar` ve `spring-boot-micrometer-metrics-test-4.1.1.jar` var.

**Testler ve test altyapısı:**
- `StreamTestSupport`:
  - `RecordingEmitter`: tek bir `CountDownLatch gate` vardır; açılınca tüm send'ler geçer.
  - `FakeUpstream`: `request(int)` boştur, `disableAutoRequestWithInitial` override edilmemiştir.
  - `CountingLease` ve `startedRegistry(Duration)` vardır. `startedRegistry` 3 argümanlı `GatewayProperties` kurar.
- `StreamBridgeTest` ve `ActiveStreamRegistryTest` `onNext` yazımının senkron olduğunu varsayar (iddialar hemen sonra gelir). `@Timeout(threadMode = SEPARATE_THREAD)` kullanırlar.
- `GracefulShutdownTest` ve `ConnectionModeConfigTest` manuel `SpringApplicationBuilder` ve `run("--lab...")` kullanır.
- `StreamEndToEndTest#clientDisconnectFreesTheStreamSlot`: `SseClient#disconnect` ile slotun boşaldığı kanıtlanmış.

**Simulator:**
- Simulator'ın `Generation.emitNext` metodu `!observer.isReady()` olduğunda durur ve `flowControlPaused()` sayar.
- In-process transport ile manuel akış kontrolünün çalıştığı simulator testinde kanıtlı: `TokenStreamServiceTest`, satır 165-202. `disableAutoRequestWithInitial(1)` sonrası `flow_control_pauses_total >= 1` ve `chunks_emitted_total == 1`.
- `SimulatorStats` alanları `active_generations`, `chunks_emitted_total` ve `flow_control_pauses_total`. `SimulatorServer.start(ServerBuilder, SimulatorConfig)` ve `stop()` public.

### Tasarım kararları

**D1. Tek doğruluk kaynağı registry.** Ayrı bir `Semaphore` kullanılmaz. `ActiveStreamRegistry` tek lock altında iki küme tutar:
- `tracked`: bugünkü `active` kümesi; boşaltma ve iptal için kullanılır.
- `admitted ⊆ tracked`: permit tutanlar.

Kapasite kontrolü `admitted.size() < maxActiveStreams` ile yapılır. `accepting` kontrolüyle aynı kritik bölgede atomik olarak çalışır.

Yeni ve değişen API'ler:
- `enum Admission { ADMITTED, REJECTED_LIMIT, REJECTED_QUEUE_TIMEOUT, REJECTED_SHUTDOWN, REJECTED_INTERRUPTED }` (package-private).
- `Admission admit(StreamBridge)`: politikayı uygular.
- `boolean tryRegister(StreamBridge)`: beklemesiz deneme; testler ve reject yolu için korunur.
- `void releasePermit(StreamBridge)`: `admitted.remove`; değiştiyse `permitFreed.signalAll()` çağırır. Idempotent'tir.
- `unregister(StreamBridge)`: `tracked.remove` ve güvenlik için `admitted.remove` yapar; `emptied` ve `permitFreed` sinyallerini verir.
- `public int admittedCount()`, `public int waitingCount()`.
- `public int activeCount()` anlamı değişmez ve tracked boyutunu döner. Mevcut testler bu anlamla geçerli kalır.

**D2. Wait modu servlet thread'inde bloklar.** `admit` registry lock'u altında şöyle çalışır:
1. `!accepting` ise `REJECTED_SHUTDOWN` döner.
2. Yer varsa iki kümeye ekler ve `ADMITTED` döner.
3. Politika `REJECT` ise `REJECTED_LIMIT` döner.
4. Politika `WAIT` ise ve `waiting >= maxWaiting` ise `REJECTED_LIMIT` döner.
5. Aksi halde `waiting++` yapar ve `permitFreed.awaitNanos(remaining)` döngüsüne girer. Her uyanışta önce `!accepting`, sonra kapasite, sonra kalan süre kontrol edilir. Süre biterse `REJECTED_QUEUE_TIMEOUT` döner. `finally` içinde `waiting--` yapılır.
6. `InterruptedException` gelirse interrupt bayrağı geri konur ve `REJECTED_INTERRUPTED` döner.

Diğer kurallar:
- `stop()`, `accepting=false` yaptığı aynı kritik bölgede `permitFreed.signalAll()` çağırır. Bekleyenler permit almadığı için `stop()` onları beklemez.
- Sıralama adil değildir: yeni gelen bir istek bekleyenin önüne geçebilir. Bu dokümante edilir.
- Servlet thread'leri `spring.threads.virtual.enabled=true` ile sanal thread'dir; bekleme platform thread tüketmez. `ReentrantLock` ve `Condition` pinning yapmaz. Bekleyen sayısı `max-waiting` ile sınırlıdır.
- Emitter ve lease beklemeden önce oluşturulur. Shared lease no-op olduğundan bu sıra korunur. İleride per-request lease gelirse sıra değişmeli; bu risk olarak not edilir.

**D3. İstek başına kuyruk ve manuel akış kontrolü (`StreamBridge`).**
- Kuyruk `ArrayBlockingQueue<TokenChunk>(streamQueueCapacity)` olur ve yalnızca `offer` kullanılır, gRPC thread'i asla bloklanmaz.
- Upstream sonu ayrı bir alanda tutulur: `volatile Status upstreamEnd`. `onCompleted` için `Status.OK` yazılır; kuyrukta kredi harcamaz.
- `beforeStart`: önce `requestStream = stream`, ardından `stream.disableAutoRequestWithInitial(capacity)`. Bu çağrı yalnızca `beforeStart` içinde geçerlidir. Sonra mevcut "claim varsa iptal et" kontrolü gelir.
- `onNext` (gRPC thread):
  - `terminal != null` ise chunk atılır.
  - `!queue.offer(chunk)` ise `overflow()` çağrılır.
  - Başarılı offer'da `metrics.chunkQueued()` ve `scheduleDrain()` çağrılır.
  - Yazım burada yapılmaz.
- `onError(t)` / `onCompleted()`: `upstreamEnd` set edilir ve `scheduleDrain()` çağrılır. Claim burada yapılmaz, böylece kuyruktaki chunk'lar önce yazılır (AC5).
- **Drainer:** Akış başına seri boşaltıcıdır. `AtomicInteger wip` ile çalışır: `scheduleDrain()` içinde `wip.getAndIncrement()==0` ise `DRAIN_EXECUTOR` (sanal thread, ad `stream-drain`) üzerinde `drainLoop` başlar. Döngü `missed = wip.addAndGet(-missed)` sıfır olana kadar sürer. Aynı akış için aynı anda tek drainer çalışır. Boşta thread tutulmaz. `sentChunks` yalnızca drainer'da değişir; happens-before ilişkisi `wip` ile sağlanır.
- `drainOnce` sırası:
  - (a) `terminal != null` ise kuyruk boşaltılıp atılır (`drainTo`), `metrics.chunksDiscarded(n)` çağrılır ve döner.
  - (b) `poll()` bir chunk dönerse `metrics.chunkDequeued()` çağrılır, `writeChunk(chunk)` yapılır ve döngü sürer.
  - (c) Kuyruk boşsa ve `upstreamEnd != null` ise `handleUpstreamEnd` çağrılır.
- `writeChunk` her zaman `sendLock.lock()` altında çalışır, bugünkü `onNext` gövdesiyle aynıdır:
  - `terminal != null` ise döner.
  - `last` ise: `lastSeen=true`, CAS `COMPLETED`, kaybederse döner, kazanırsa `releaseCapacity()`. **Yeni:** permit ve lease yazımdan önce bırakılır.
  - Chunk `send` edilir; `last` ise ardından `complete` gönderilir, `completeEmitter()` ve `finish(COMPLETED)` çağrılır.
  - Hata yolunda `last` ise `terminal.set(CLIENT_GONE)` + `finish(CLIENT_GONE)` (ikinci `releaseCapacity` yok); değilse `clientGone()`.
  - Başarılı ve `last` olmayan yazımdan sonra, **lock dışında** ve `terminal==null` ise `requestStream.request(1)` çağrılır.
- **Kredi değişmezi:** `outstanding + queued + inWrite == capacity`. Kuyruk kapasiteyi aşamaz. Upstream, `outstanding==0` iken (in-process'te ve HTTP/2 penceresi dolunca) durur.
- `handleUpstreamEnd(status)`:
  - OK ve `lastSeen` ise no-op'tur; terminal zaten `COMPLETED`'tır.
  - OK ve `lastSeen` değilse CAS `UPSTREAM_ERROR` → `releaseCapacity()` → `sendErrorAndComplete(INTERNAL, "upstream ended without last chunk")` → `finish`.
  - Hata durumunda CAS `UPSTREAM_ERROR` → `releaseCapacity()` → `sendErrorAndComplete(code, desc)` → `finish`.
  - Upstream zaten bittiği için iptal gerekmez.
- `overflow()` (gRPC thread; upstream krediyi ihlal etti):
  1. CAS `UPSTREAM_ERROR`.
  2. `cancelUpstream`.
  3. `releaseCapacity()`.
  4. `TERMINAL_EVENT_EXECUTOR` üzerinde `sendErrorAndComplete("RESOURCE_EXHAUSTED", "per-request queue overflow")` + `finish`.
  5. `scheduleDrain()` (atma için).
- `clientGone()` ve `cancelForShutdown()`: bugünkü gibi CAS → `cancelUpstream` → `releaseCapacity()` → `finish(outcome)`. Tracked kümeden hemen çıkış korunur. Ek olarak `scheduleDrain()` çağrılır ve kuyruk atılır.
- `release(outcome)` ikiye bölünür:
  - `releaseCapacity()`: `lease.close()` + `registry.releasePermit(this)`. Yalnızca CAS kazananı, tam bir kez çağırır.
  - `finish(outcome)`: `registry.unregister(this)` + INFO log.
- `sendLock` ve `tryLock(5s)` semantiği değişmez. Shutdown'ın error yazıcısı, drainer'ın takılı yazımını en fazla 5s bekler.
- Yarış durumu: `onNext`, terminal kontrolünü geçip claim'den sonra enqueue ederse, kendi `scheduleDrain()` çağrısıyla drainer bu öğeyi (a) adımında atar. Kuyruk ve metrik tutarlı kalır.
- Constructor: `StreamBridge(String requestId, SseEmitter emitter, ActiveStreamRegistry registry, ChannelProvider.Lease lease, int queueCapacity, StreamMetrics metrics)`. Test kolaylığı için package-private `int queuedChunks()` eklenir.

**D4. Takılı yazım ve kapasite (açıkça).**
- **Takılı terminal yazımı:** Permit ve lease claim anında bırakılır; kapasite sızıntısı biter (AC6). Akış yazım dönene kadar tracked kalır. `stop()` bunu drain süresi boyunca bekler ve "still hold a slot" WARNING'ini loglar; mesaj "still finishing a final write" olarak güncellenir. Bedel bir sanal drainer thread'i ve açık HTTP yanıtıdır.
- **Takılı ara chunk yazımı:** Terminal claim yoktur. Upstream'in deadline ya da error sinyali kuyruğun arkasında bekler. Bu nedenle permit şu durumlardan birine kadar tutulur:
  - Emitter timeout'u (deadline + 5s) `clientGone` callback'ini tetikler. Bu **doğrulanmamış hipotezdir**: blokajlı bir yazım sırasında `onTimeout` callback'inin gerçekten çalıştığı test edilmedi.
  - Shutdown iptali gelir.
  - Yazım döner.

  Bu yeni bir gerileme değildir: bugün de takılı `onNext`, gRPC callback'lerini seri sıraya sokup `onError` teslimini engelliyordu. Fark şudur: artık bloklanan gRPC thread'i değil drainer'dır, upstream ise akış kontrolüyle durur. Bu madde README'de açıkça yazılır.

**D5. Metrikler (`stream/StreamMetrics`, public, `@Component`).**
- Constructor `(MeterRegistry, ActiveStreamRegistry)`. Registry metriklere bağımlı değildir, böylece bağımlılık döngüsü olmaz.
- Gauge'lar:
  - `Gauge.builder("active_streams", registry, ActiveStreamRegistry::admittedCount)`.
  - `Gauge.builder("queue_depth", this, m -> m.queuedChunks.get()).tag("queue","chunks")`. `AtomicInteger queuedChunks` alanı bean'de durur ve tüm akışların toplamıdır.
  - `Gauge.builder("queue_depth", registry, ActiveStreamRegistry::waitingCount).tag("queue","admission")`.
- Gauge durum nesneleri Spring singleton'ıdır, yani güçlü referansla tutulur; Micrometer'ın zayıf referansı sorun çıkarmaz.
- Counter: `rejected_total` ve tag `reason`. Metot `rejected(String reason)`. Sayaçlar önceden register edilir; böylece sıfır değerli sayaç da okunabilir.
- Metrik adları istekte verildiği gibi kullanılır; `/actuator/metrics/<ad>` ile görünür. İleride bir Prometheus registry eklenirse `_total` son eki nasıl normalize edilecek, doğrulanmadı. Bu bir varsayımdır.
- Red sayımı `StreamController` içinde yapılır: `Admission` → `reason`. Lease alma hatası (`IllegalStateException`) `upstream_unavailable` olarak sayılır. 400 yanıtları sayılmaz.

**D6. Konfigürasyon.** `GatewayProperties` dördüncü bileşen olarak `@DefaultValue Limits limits` alır:
```java
public enum OverloadPolicy { REJECT, WAIT }
public record Limits(
        @DefaultValue("100") int maxActiveStreams,       // >= 1
        @DefaultValue("reject") OverloadPolicy overloadPolicy,
        @DefaultValue("5s") Duration queueWait,          // pozitif (her iki modda doğrulanır)
        @DefaultValue("100") int maxWaiting,             // >= 1, yalnızca WAIT'te kullanılır
        @DefaultValue("16") int streamQueueCapacity) {   // 1..4096
    public Limits { /* IllegalArgumentException, mesajda lab.limits.<ad> */ }
}
```
- İstek başına kuyruk property'si `lab.limits.stream-queue-capacity` olur.
- `max-waiting` istekte yoktu. README'deki "queues ... are bounded" ilkesi gereği bekleme kuyruğu da sayıca sınırlanır. Kullanıcı isterse kaldırılabilir; bu durumda bekleyen sayısı yalnızca Tomcat bağlantı limitiyle sınırlı kalır.

## 3. Uygulama adımları

1. `config/GatewayProperties.java`: `OverloadPolicy` ve `Limits` eklenir, record'a `limits` bileşeni eklenir.
2. `application.yaml`: `lab.limits` bloğu eklenir: `max-active-streams: 100`, `overload-policy: reject`, `queue-wait: 5s`, `max-waiting: 100`, `stream-queue-capacity: 16`.
3. `stream/ActiveStreamRegistry.java`: D1 ve D2 uygulanır: `tracked`/`admitted` kümeleri, `permitFreed` Condition, `waiting` sayacı, `admit`, `releasePermit`, `admittedCount`, `waitingCount`. `stop()` içinde `signalAll` eklenir ve WARNING metni güncellenir. Faz ve süreler değişmez.
4. `stream/StreamMetrics.java` (yeni) oluşturulur (D5).
5. `stream/StreamBridge.java`: D3 uygulanır. Kuyruk, drainer, `releaseCapacity`/`finish` ayrımı, `overflow`, `beforeStart` içinde `disableAutoRequestWithInitial` ve yeni constructor eklenir. Sınıf javadoc'u güncellenir.
6. `stream/StreamController.java`:
   - `StreamMetrics` ve `limits` inject edilir.
   - `tryRegister` yerine `registry.admit(bridge)` kullanılır.
   - Red durumunda `lease.close()`, `metrics.rejected(reason)` ve `unavailable()` çağrılır.
   - Lease hatasında `rejected("upstream_unavailable")` sayılır.
7. Test desteği, `StreamTestSupport.java`:
   - `startedRegistry(Duration)` korunur.
   - Yeni `startedRegistry(Duration drain, Limits limits)` eklenir.
   - Varsayılan `Limits` yardımcısı eklenir.
   - `FakeUpstream` şunları kaydeder: `initialRequest` (`disableAutoRequestWithInitial` override) ve `AtomicInteger requested`; ayrıca `credit()` yardımcısı eklenir.
   - Yeni `SteppingEmitter`: her `send` bir `Semaphore` permit'i alır ve `sendsStarted` sayacı tutar.
   - `newBridge(...)` yardımcısı `StreamMetrics(new SimpleMeterRegistry(), registry)` ile kurar.
8. Mevcut testlerin uyarlanması:
   - `StreamBridgeTest`: `onNext` sonrası iddialar `await().atMost(TIMEOUT).untilAsserted(...)` ile beklenir. İddiaların içeriği değişmez.
   - `ActiveStreamRegistryTest`: yeni constructor kullanılır.
   - `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml`: limit varsayılanları eklenir.
9. Yeni ve genişletilmiş testler eklenir (Bölüm 4'teki tablo).
10. `README.md` güncellenir:
    - Ayar tablosuna `lab.limits.*` eklenir.
    - "Bounded resources" alt bölümü eklenir: politikalar, 503 nedenleri, metrikler ve tag'ler.
    - "Known limits" yeniden yazılır: slot maddesi yerine D4 içeriği gelir; Prometheus registry'nin olmadığı not edilir.
    - Modül tablosunda "bounded queue pending" ifadesi güncellenir.
11. `proto/`, `simulator/`, `LifecyclePhases`, `upstream/*` dosyalarına dokunulmaz. `pom.xml` değişmez; `SimpleMeterRegistry` micrometer-core içindedir ve compile classpath'te bulunur.

## 4. Doğrulama

Tüm zamanlamalı testler `@Timeout(value=..., threadMode = SEPARATE_THREAD)` taşır. Gate'ler ve semaphore'lar `finally` içinde açılır. `Thread.sleep` kullanılmaz; bekleme Awaitility ile yapılır. Süre üst sınırları cömerttir.

| AC | Test | Beklenen |
|---|---|---|
| AC1 | `ActiveStreamRegistryTest#rejectPolicyRejectsBeyondMaxActiveStreams` | max=1 iken ikinci `admit` `REJECTED_LIMIT` döner; `releasePermit` sonrası `ADMITTED` |
| AC1, AC7 | `OverloadPolicyTest#rejectPolicyAnswers503AndCountsRejection`. Manuel builder, `--lab.limits.max-active-streams=1`. HANG akışı slotu tutar. | İkinci GET 503 ve `Retry-After` alır, content-type SSE değildir. `rejected_total{reason=limit}==1`, `active_streams==1`. `disconnect` sonrası `active_streams==0` olur ve yeni akış `complete` ile biter. `/actuator/metrics/rejected_total` 200 döner. |
| AC2 | `ActiveStreamRegistryTest#waitingRequestIsAdmittedWhenAPermitIsReleased`, `#waitingRequestTimesOutAfterQueueWait` (500ms), `#waitersBeyondMaxWaitingAreRejectedImmediately` | Bekleyen `ADMITTED` alır. Timeout'ta `REJECTED_QUEUE_TIMEOUT` ve geçen süre `>= 500ms` ve `< 5s`. `max-waiting=1` iken ikinci bekleyen hemen `REJECTED_LIMIT` alır. `waitingCount()` Awaitility ile izlenir. |
| AC2, AC7 | `OverloadPolicyTest#waitPolicyAdmitsWhenSlotFrees`, `#waitPolicyTimesOutWith503` | İlk akış kopunca bekleyen 200 alıp `complete` ile biter. Diğer testte 503 döner ve `rejected_total{reason=queue_timeout}==1`. Bekleme sırasında `queue_depth{queue=admission}==1`. |
| AC3 | `ActiveStreamRegistryTest#stopReleasesWaitersWithoutDelayingShutdown` (queue-wait 30s, drain 1s) | Bekleyen `stop()` başladıktan sonra 2s içinde `REJECTED_SHUTDOWN` alır. `stop()` 4s altında biter. |
| AC3 | `OverloadPolicyTest#shutdownReleasesWaitingRequests` (wait, queue-wait 30s, drain 2s, HANG slotu tutar) | Bekleyen istek 10s içinde 503 alır, `rejected_total{reason=shutdown}==1`. `close()` 11s'den kısa sürer (30s'lik beklemeden etkilenmez). HANG akışı tek `error` CANCELLED alır. |
| AC3, AC9 | `GracefulShutdownTest` (değişmeden) | Mevcut iddialar geçer |
| AC4 | `StreamBridgeTest#beforeStartDisablesAutoRequestWithQueueCapacity` | `initialRequest == capacity` |
| AC4, AC7 | `StreamBridgeTest#slowConsumerQueueNeverExceedsCapacity` (capacity 4, `SteppingEmitter`). Test thread'i kredi-uyumlu bir üretici gibi davranır: yalnızca `upstream.credit() > 0` iken `onNext` çağırır ve her çağrıdan sonra `queuedChunks()` ile gauge'u örnekler. | Örneklenen her değer `<= 4`. Tüketici durduğunda kredi 0'a iner ve üretici ilerleyemez. Semaphore adım adım açılınca `requested` artar. Sonunda tüm seq'ler sırayla gelir, ardından tek `complete`. `queue_depth{chunks}==0`. |
| AC4 | `UpstreamFlowControlTest#realUpstreamPausesWhileClientIsStalled` (Spring yok; in-process `SimulatorServer`, gerçek stub, `StreamBridge` + `RecordingEmitter.stalled()`, capacity 4, chunks 50). Slow-consumer için tek uçtan uca test budur. | Awaitility ile `flow_control_pauses_total >= 1` beklenir; `chunks_emitted_total <= 4`. Gate açılınca 50 chunk ve `complete` gelir. |
| AC5 | `StreamBridgeTest#upstreamErrorIsDeliveredAfterQueuedChunks`, `#chunkBeyondGrantedCreditFailsWithResourceExhausted`, `#shutdownDiscardsQueuedChunks`; mevcut `#nothingIsSentAfterTheTerminalEvent` | Kuyruktaki chunk'lar `error`'dan önce gelir. Kredi ihlalinde tek `error` RESOURCE_EXHAUSTED, upstream iptal edilir ve kuyruk kapasiteyi aşmaz. Shutdown'da kuyruk atılır ve gauge 0 olur. Terminalden sonra olay gelmez. |
| AC5, AC9 | `StreamEndToEndTest` (değişmeden) | Sıra, tek terminal ve hata kodları manuel akış kontrolüyle de korunur |
| AC6 | `StreamBridgeTest#terminalClaimReleasesPermitAndLeaseBeforeTheFinalWrite` ve `#stuckUpstreamErrorWriteDoesNotHoldThePermit`; `ActiveStreamRegistryTest#stuckFinalWriteDoesNotBlockAdmissionOfANewStream` (max=1). Mevcut `#streamWithAFinalWriteInProgressKeepsItsSlotButDoesNotDelayStop` ve `StreamBridgeTest#shutdownCannotCancel...`, `#failedWriteOfTheFinalChunkStillFreesTheSlot` yeşil kalır. | Gate kapalıyken `admittedCount()==0`, `activeCount()==1`, `CountingLease.closes==1`. Yeni `tryRegister` true döner. Gate açılınca `activeCount()==0` olur. Lease ikinci kez kapanmaz. |
| AC8 | `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` (genişletildi) ve yeni `#invalidLimitsFailAtStartup` (`@ParameterizedTest`: `max-active-streams=0`, `max-waiting=0`, `stream-queue-capacity=0`, `stream-queue-capacity=5000`, `queue-wait=0s`, `overload-policy=drop`) | Varsayılanlar eşleşir. Her geçersiz değerde `hasStackTraceContaining("lab.limits.")` |
| AC9 | Coordinator `git diff --stat -- proto simulator` ve `grep -rn "Thread.sleep" gateway/src` çalıştırır | İkisi de boş |
| AC10 | README okunarak kontrol edilir | D4'teki iki durum ve doğrulanmamış hipotez açıkça yazılı |

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- Tam doğrulama: `./mvnw -B verify`
- Odaklı: `./mvnw -B -pl gateway -am verify -Dtest="StreamBridgeTest,ActiveStreamRegistryTest,UpstreamFlowControlTest,OverloadPolicyTest,ConnectionModeConfigTest" -Dsurefire.failIfNoSpecifiedTests=false`
- Kararlılık için `./mvnw -B -pl gateway -am test` en az iki kez tekrarlanır.

**Riskler:**
- **Async geçiş:** Yazımlar gRPC thread'inden drainer'a taşındığı için mevcut senkron test varsayımları kırılır. Testler Awaitility ile uyarlanmalı, iddialar gevşetilmemeli.
- **Shutdown ile final yazım yarışı:** `COMPLETED` claim'inde permit erken bırakıldığı için shutdown sırasında yeni bir akış kabul edilemez (`accepting=false`). Ancak normal çalışmada tamamlanmakta olan akışlar kısa süreliğine permit'siz açık HTTP yanıtı olarak kalır. Bu nedenle limit "upstream çağrısı ve lease" sınırıdır, "açık yanıt" sınırı değildir; README'de yazılır.
- **WAIT modunda thread tüketimi:** WAIT modu servlet thread'ini bloklar. Sanal thread kapatılırsa platform thread havuzunu `max-waiting` kadar tüketir.
- **Sıralama adil değil:** Yeni gelen istek bekleyenin önüne geçebilir.
- **Gerçek TCP kanıtı yok:** Takılı istemci davranışı yine fake emitter ile test edilir.
- **Prometheus yok:** `prometheus` exposure etkisiz kalır; bilinçli olarak dokunulmaz.

**Doğrulanmamış varsayımlar (implementer `.m2` jar'larına karşı kontrol etmeli):**
- `ClientCallStreamObserver.disableAutoRequestWithInitial(int)`, grpc-stub 1.81.0'da varsayılan gövdeli (abstract değil) bir metot olarak var. `FakeUpstream` bunu override etmeli.
- `request(int)` ve `cancel`'ın gRPC dışı thread'den çağrılması güvenli. `cancel` bugün de böyle çağrılıyor; `request` için `ClientCall` javadoc'u kontrol edilmeli.
- Micrometer 1.17.1 paketleri: `io.micrometer.core.instrument.{MeterRegistry, Gauge, Counter}` ve `io.micrometer.core.instrument.simple.SimpleMeterRegistry`.
- Manuel `SpringApplicationBuilder` context'inde bir `MeterRegistry` bean'i bulunuyor (Boot 4 simple fallback). Metrik e2e testi, `@SpringBootTest`'in metrik export davranışına bağlı kalmamak için bilerek manuel builder ile yazıldı. `@SpringBootTest` gerekirse `spring-boot-micrometer-metrics-test` içindeki annotasyon kontrol edilmeli.
- Spring async timeout, handler emitter'ı döndürdükten sonra başlar; wait süresi emitter timeout'una sayılmaz.
- Emitter `onTimeout` callback'i, başka bir thread `send` içinde bloklanmışken de çalışır (D4). Bu hipotez README'de doğrulanmamış olarak kalır.
- Tomcat graceful phase'in açık async yanıtları beklemesi önceki feature'dan beri hipotezdir.

**Kapsam dışı:** timeout ve iptal zincirinin simulator istatistikleriyle doğrulanması, reconnect/retry, per-request bağlantı modu, loadgen, MQTT.

**Açık sorular (engelleyici değil):**
- `lab.limits.max-waiting` ve `queue_depth{queue=admission}` tag'i istekte geçmiyordu. Bekleme kuyruğu da sınırlı olsun diye eklendi; kullanıcı isterse çıkarılabilir.
- Metrik adları istekteki gibi `active_streams`, `queue_depth`, `rejected_total` olarak kullanılır. Prefix eklenmedi.
