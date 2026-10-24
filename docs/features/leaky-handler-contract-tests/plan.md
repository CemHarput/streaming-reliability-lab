# Plan: leaky-handler-contract-tests

Kaynak kodda doğrulanan olgular (HEAD 3187d71): `StreamBridge` ve onunla ilgili tüm tipler package-private ve `io.github.cemharput.streamlab.gateway.stream` paketinde. `ActiveStreamRegistry` public ve final değil. `releasePermit(StreamBridge)` package-private, final değil ve idempotent. `StreamTimeouts.pendingTimers()` public; zamanlayıcı `removeOnCancel` ile çalışıyor. `StreamTimer(StreamTimeouts, Consumer<TimeoutKind>)` package-private. `RecordingEmitter` olayları `"event:<name>\ndata:..."` metni olarak kaydediyor (`UpstreamFlowControlTest` `contains("event:chunk")` ile buna dayanıyor). `FAULT_TYPE_HANG`, `at_seq`'e kadar chunk gönderiyor, sonra yalnızca cancel, deadline ya da server shutdown ile bitiyor. In-process transport istemcinin `request()` kredisine uyuyor: `UpstreamFlowControlTest#realUpstreamPausesWhileClientIsStalled` `chunksEmittedTotal <= capacity` doğruluyor. Gerçek `clientGone()` SSE terminal olayı yazmıyor. README:117-118 de bunu söylüyor: "exactly one complete or error event, except when the client disconnected". Kökteki `pom.xml` içinde surefire paralel ayarı ve JaCoCo eşiği yok. `.claude/project-context.md` hâlâ boş bir şablon. Bu plan yalnızca yukarıdaki doğrulanmış koda dayanıyor.

## 1. Davranış ve kabul kriterleri

Uygulamadan bağımsız tek bir akış sözleşmesi, aynı senaryolarla iki uygulamada parametrik olarak çalışır: `REAL` (`StreamBridge`) ve `LEAKY` (yalnızca test kodunda duran `LeakyStreamHandler`). Sözleşme bir ihlal listesi üretir; test, bu listeyi beklenen ihlaller tablosuyla iki yönde karşılaştırır. Build yeşil kalır. LEAKY'nin iki kusuru her çalıştırmada deterministik olarak görünür.

- **AC1** Üretim kodunda, `proto/` ve `simulator/` altında değişiklik yok. `git diff --stat -- gateway/src/main proto simulator` boş. Yeni ya da değişen dosyalar yalnızca `gateway/src/test/...` altında ve `docs/` ile `README.md` içinde.
- **AC2** `StreamContractTest` gerçek in-process simulatör ve gerçek stub ile 6 senaryoyu (COMPLETION, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN_CANCEL, FIRST_CHUNK_TIMEOUT, SLOW_CONSUMER) her iki uygulamada çalıştırır. REAL'de her senaryonun ihlal listesi boştur.
- **AC3** LEAKY / CLIENT_GONE'da tespit edilen küme tam olarak `{UPSTREAM_CANCELLED}` olur. `active_generations` 2 s boyunca 1'de kalır, `cancelled_total` 0 kalır. Lease 1 kez kapanır, permit 1 kez bırakılır, `pendingTimers()` 0 olur, istemci koptuktan sonra hiçbir olay gönderilmez. LEAKY'nin diğer 5 simülatör senaryosunda hiç ihlal yoktur.
- **AC4** `TerminalRaceContractTest`, LEAKY'de iç bariyerle şunları gösterir. COMPLETION_VS_SHUTDOWN'da 5 iterasyonun her birinde küme tam olarak `{SINGLE_TERMINAL, RELEASE_ONCE}` olur: 2 terminal olay, `leaseCloses == 2`, `permitReleases == 2`. COMPLETION_VS_CLIENT_GONE'da her iterasyonda küme tam olarak `{RELEASE_ONCE}` olur. Bariyer her iterasyonda iki tarafça geçilir (`raceHookBroken() == false`).
- **AC5** REAL aynı iki yarış senaryosunu 200'er iterasyonla stres olarak çalıştırır. Her iterasyonda ihlal listesi boştur; sonunda `pendingTimers() == 0` olur.
- **AC6** Beklenen ihlaller tablosu her `(Kind, Scenario)` çiftini kapsar ve iki yönde doğrulanır. Beklenen bir ihlal görülmezse test kırılır; beklenmeyen bir ihlal görülürse test kırılır. Bir meta test, tablonun iki kusuru ve yalnızca onları içerdiğini doğrular.
- **AC7** Test hijyeni sağlanır: `Thread.sleep` yok. Her yeni sınıfta `@Timeout(value = 60, threadMode = SEPARATE_THREAD)` var. Gate, latch ve bariyerler `finally` içinde açılır. Başlatılan her `StreamTimeouts`, simülatör ve kanal kapatılır. "Hiçbir şey olmaz" pencereleri en az 5 kat pay taşır.
- **AC8** `docs/failure-analysis.md`, `docs/adr/0002-single-terminal-claim.md` ve README işaretçileri eklenir. Her cümle koda uyar; doğrulanmamış iddialar "hipotez" diye işaretlenir.
- **AC9** `./mvnw -B verify` yeşil geçer: simulator 12 test, gateway 165 + yeni testler. Mevcut test dosyaları yalnızca `StreamTestSupport` içine ekleme yapılarak değişir; hiçbir test silinmez ya da zayıflatılmaz.
- **AC10** Yeni iki sınıfın toplam süresi yaklaşık 40 s'yi geçmez; ölçülen süre implementation.md'ye yazılır.

## 2. Dosyalar, semboller ve tasarım

### Doğrulanmış giriş noktaları
- `StreamBridge`: constructor (package-private), `start()`, `cancelForShutdown()`, `bufferedChunks()`, `queuedChunks()`. Emitter callback'leri `onCompletion`, `onTimeout` ve `onError` yoluyla `clientGone()` çağırır. Terminal sonucu `claim()` CAS ile alır ve bu sırada `timer.close()` yapılır. `releaseCapacity()` içinde `lease.close()` ve `registry.releasePermit(this)` çağrılır.
- `ActiveStreamRegistry`: `tryRegister`, `releasePermit`, `admittedCount`, `stop()`. `stop()` sırasıyla `awaitEmpty(drainTimeout)`, `cancelAll` ve `FORCED_CANCEL_WAIT` (2 s) adımlarını izler.
- `StreamTimer`, `StreamTimeouts`, `TimeoutKind.message()`, `SseEvents.{CHUNK, COMPLETE, ERROR, ChunkEvent, CompleteEvent, ErrorEvent}`, `StreamBridge.SHUTDOWN_MESSAGE`, `RetryPolicy.disabled()`, `UpstreamStarter`.
- `StreamTestSupport`: `RecordingEmitter` (`responsive()`, `stalled()`, `gate`, `simulateClientGone()`, `events`, `completed`), `CountingLease`, `FakeUpstreamStarter` (`calls`, `Call.observer()`, `Call.upstream()`), `FakeUpstream.deliver(observer, chunk)`, `chunk(seq, last)`, `startedTimeouts`, `properties(...)`, `defaultLimits()`.
- `UpstreamFlowControlTest` deseni: her test için `SimulatorServer.start(InProcessServerBuilder.forName(name), new SimulatorConfig(0, Duration.ofSeconds(2), 2, RequestLimits.DEFAULT))`, ayrı veri kanalı ve admin kanalı. `tearDown` sırasıyla kanallarda `shutdownNow().awaitTermination` sonra `server.stop()` çağırır.

### Karar 1: Ayrım noktası ve hangi parçanın gerçek olduğu
`StreamUnderTest` adında yalnızca testte duran bir arayüz tanımlanır. Her iki uygulama da kendisine verilen bir `UpstreamStarter` üzerinden çağrı başlatır. Böylece simülatör senaryolarında aynı gerçek stub lambda'sı kullanılır: `TokenStreamServiceGrpc.newStub(channel).withDeadlineAfter(deadline.toNanos(), NANOSECONDS).generateTokens(req, obs)`. Yarış senaryolarında ise `FakeUpstreamStarter` kullanılır.

```java
interface StreamUnderTest {
    enum Kind { REAL, LEAKY }
    record Wiring(GenerateRequest request, RecordingEmitter emitter, CountingLease lease,
                  StreamTimeouts timeouts, int capacity, UpstreamStarter starter,
                  Duration deadline, Duration drainTimeout) {}
    static StreamUnderTest create(Kind kind, Wiring wiring);
    boolean admit();
    void start();
    void clientGone();               // wiring.emitter().simulateClientGone()
    Duration shutdown();             // REAL: registry.stop(); LEAKY: cancelForShutdown(); süreyi döner
    void cancelForShutdown();        // yarış için doğrudan çağrı (registry.cancelAll de bunu çağırır)
    boolean armTerminalRaceHook(CountDownLatch arrivals); // LEAKY: kurar, true; REAL: false
    boolean raceHookBroken();        // REAL: false
    int leaseCloses(); int permitReleases(); int permitsHeld();
    int pendingTimers();             // wiring.timeouts().pendingTimers()
    int bufferedChunks();            // REAL: bridge.bufferedChunks(); LEAKY: received - creditReturned
}
```

Her ölçümde neyin gerçek, neyin sahte olduğu:

| Ölçüm | REAL | LEAKY |
|---|---|---|
| upstream çağrısı | gerçek in-process simülatör (yarışta `FakeUpstream`) | aynısı |
| emitter ve olay günlüğü | `RecordingEmitter` (sahte istemci) | aynısı |
| lease | `CountingLease` | aynısı |
| permit | yeni `CountingRegistry extends ActiveStreamRegistry`; `releasePermit` override edilip sayılır, `admittedCount()` "held" değeridir | `LeakyStreamHandler.PermitLedger` (acquired / releases sayaçları; held = acquired - releases, çift release'te -1 olur) |
| zamanlayıcı | test başına yeni `StreamTimeouts`; `pendingTimers()` | aynı `StreamTimeouts`; LEAKY üretimdeki `StreamTimer`'ı kullanır |
| madde (3) | ayrı admin kanalıyla `GetStats` (`active_generations`, `cancelled_total`, `completed_total`, `faulted_total`) | aynısı |

Test başına yeni bir simülatör kullanılır (`@BeforeEach`). Böylece sayaçlar başka testlerin generation'larını içermez ve `started_total == 1` ön koşulu anlamlı olur.

Üretim değişikliği gerekmiyor; hiçbir test erişimcisi eklenmiyor. Gerekçeler: terminal sonuç SSE olaylarından ölçülüyor; `pendingTimers()` zaten public; permit sayımı package-private `releasePermit`'in aynı paketteki bir alt sınıfta override edilmesiyle yapılıyor; `bufferedChunks()` ve `queuedChunks()` zaten mevcut. Tüm yeni sınıflar `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/` altında. Package-private erişim gerektiği için bu paket zorunlu.

### Karar 2: Sözleşme veri yapısı (`StreamContract.java`)
- `enum Clause { SINGLE_TERMINAL /*1*/, RELEASE_ONCE /*2*/, UPSTREAM_CANCELLED /*3*/, NO_TIMER_LEAK /*4*/, QUEUE_BOUNDED /*5*/ }`
- `record Violation(Clause clause, String detail)`
- `record Observation(List<String> events, int eventsAtTrigger, int leaseCloses, int permitReleases, int permitsHeld, int pendingTimers, int maxInFlight, int maxBuffered, Boolean upstreamStopped, long active, long cancelled, long completed, long faulted, Duration triggerTook)`. Ölçülmeyen alanlarda -1 ya da `null` bulunur.
- `enum Scenario` (8 değer) ve her senaryonun beklenen terminal şekli: `COMPLETE`, `ERROR(code)`, `NONE_AFTER_TRIGGER`, `EXACTLY_ONE_ANY`, `AT_MOST_ONE`. Ayrıca hangi maddelerin ölçüldüğü tutulur; ölçülmeyen madde N/A sayılır.
- `static List<Violation> check(Scenario s, Observation o, int capacity)` hiçbir zaman exception atmaz, ihlal listesi döner. Terminal olay, `"event:complete"` ya da `"event:error"` içeren olaydır.
- `static final Map<Scenario, Set<Clause>> EXPECTED_LEAKY`. REAL için her senaryoda `Set.of()` beklenir.
- Test tarafında `assertMatchesExpectation(kind, scenario, violations)` şunu yapar: `assertThat(clausesOf(violations)).as(report).isEqualTo(expected)`. Mesaj tür, senaryo, beklenen küme ve detaylı ihlallerden oluşur. Ayrıca ihlaller `System.Logger` ile INFO seviyesinde loglanır; LEAKY ihlalleri böylece build çıktısında görünür. Küme eşitliği iki yönü birlikte sağlar: beklenen ama görülmeyen ihlal testi kırar, görülen ama beklenmeyen ihlal de kırar.

### Madde tablosu (ne ölçülür, hangi koşul aranır)

| Madde | Ölçüm | İhlal koşulu |
|---|---|---|
| (1) SINGLE_TERMINAL | `events` | `COMPLETE`/`ERROR(code)` beklenirken terminal sayısı ≠ 1, tür ya da kod yanlış, ya da beklenen chunk sayısı eksik. `EXACTLY_ONE_ANY`'de terminal sayısı ≠ 1. `AT_MOST_ONE`'da 2 ya da daha fazla. `NONE_AFTER_TRIGGER`'da `events.size() != eventsAtTrigger`. Her durumda bir terminal olaydan sonra herhangi bir olay varsa ihlal. |
| (2) RELEASE_ONCE | `leaseCloses`, `permitReleases`, `permitsHeld` | `leaseCloses != 1`, `permitReleases != 1` ya da `permitsHeld != 0` |
| (3) UPSTREAM_CANCELLED | `upstreamStopped` (`CANCEL_WINDOW` = 2 s içinde `active == 0`) ve sayaçlar | durmadıysa ihlal. Cancel senaryolarında (CLIENT_GONE, SHUTDOWN, TIMEOUT) ek olarak `cancelled != 1` ya da `completed != 0`. COMPLETION/SLOW_CONSUMER'da `completed != 1`, UPSTREAM_ERROR'da `faulted != 1`. SHUTDOWN'da `triggerTook > drainTimeout + FORCED_CANCEL_WAIT + 3 s`. Yarışlarda N/A. |
| (4) NO_TIMER_LEAK | `pendingTimers` | `!= 0` |
| (5) QUEUE_BOUNDED | durmuş istemci penceresinde örneklenen `maxInFlight` = max(`chunksEmittedTotal` − yazılan chunk olayı) ve `maxBuffered` | herhangi biri `> capacity`. Yalnızca SLOW_CONSUMER'da ölçülür. |

`maxInFlight` örneklemesinde önce simülatör istatistikleri, sonra `events` okunur. Böylece iki okuma arasında yapılan yazmalar ölçümü şişiremez. Örnekleme yalnızca gate kapalıyken yapılır; bu, mevcut testin doğruladığı rejimle aynıdır.

### Senaryo tablosu
Ortak değerler: capacity 16 (SLOW_CONSUMER'da 4), deadline 60 s, zamanlayıcılar 30 s/30 s (TIMEOUT'ta first-chunk 300 ms), `drainTimeout` 200 ms, `QUIET` 500 ms, `TIMEOUT` 10 s.

| Senaryo | İstek | Ön koşul (tek bir `untilAsserted` ile beklenir; hata harness hatası sayılır) | Tetik | Süre sınırı |
|---|---|---|---|---|
| COMPLETION | 5 chunk, 1 ms aralık | yok | yok | `atMost` 10 s (beklenen < 100 ms) |
| UPSTREAM_ERROR | 5 chunk, INTERNAL @2 | yok | yok | 10 s |
| CLIENT_GONE | 10 chunk, HANG @2 | 2 chunk olayı, `active == 1`, `started == 1`, `pendingTimers == 1` (IDLE kurulu, bu madde (4)'ü boş geçmekten korur) | `clientGone()` | `CANCEL_WINDOW` 2 s (in-process cancel ms mertebesinde, ≥ 20x pay) |
| SHUTDOWN_CANCEL | 10 chunk, HANG @2 | CLIENT_GONE ile aynı | `shutdown()` | stop sınırı 0,2 + 2 + 3 s (beklenen yaklaşık 0,2 s) |
| FIRST_CHUNK_TIMEOUT | 5 chunk, HANG @0, first-chunk 300 ms | yok | zamanlayıcı | 10 s (yaklaşık 33x) |
| SLOW_CONSUMER | 50 chunk, 1 ms aralık, `stalled()` emitter, capacity 4 | `flowControlPausesTotal >= 1` | 500 ms örnekleme penceresi (`during(...).until(sample)`, `pollInterval` 20 ms), sonra `gate.countDown()` | 10 s |
| RACE_COMPLETION_VS_SHUTDOWN | `FakeUpstreamStarter` | `starter.calls.size() == 1` | A: `deliver(chunk(0,false))`, `deliver(chunk(1,true))`, `onCompleted()`. B: `cancelForShutdown()` | future `get` 10 s |
| RACE_COMPLETION_VS_CLIENT_GONE | aynısı | aynısı | A aynı; B: `clientGone()` | aynısı |

Her simülatör senaryosunun akışı şöyledir: `create`, `admit()` (true olmalı; değilse harness hatası), `start()`, ön koşul, tetik (`eventsAtTrigger` ve `triggerTook` kaydedilir), `upstreamStopped = awaitQuietly(CANCEL_WINDOW, active == 0)`, ardından `awaitStable(QUIET)`. `awaitQuietly`, `ConditionTimeoutException`'ı yakalayıp false döner. `awaitStable` tüm ölçümleri tek bir `Observation` anlık görüntüsünde toplar; `await().during(QUIET).atMost(QUIET × 10).until(() -> snapshot().equals(last.getAndSet(snapshot())))` ile bekler ve değerlendirmeyi pencere içinde alınan son anlık görüntü üzerinde yapar. Bu sayede beklenen sinyalden sonra yazılan değerler sonradan ayrıca okunmaz. Ardından `check` ve `assertMatchesExpectation` çalışır. Bu bekleme hiçbir beklentiyi kodlamaz, yalnızca ölçümlerin durulmasını bekler; bu yüzden iki uygulama için de aynıdır.

### Beklenen ihlaller tablosu (LEAKY; REAL'de her satır boş küme)

| Senaryo | (1) | (2) | (3) | (4) | (5) |
|---|---|---|---|---|---|
| COMPLETION | sağlanır | sağlanır | sağlanır | sağlanır | N/A |
| UPSTREAM_ERROR | sağlanır | sağlanır | sağlanır | sağlanır | N/A |
| CLIENT_GONE | sağlanır | sağlanır | **ihlal** | sağlanır | N/A |
| SHUTDOWN_CANCEL | sağlanır | sağlanır | sağlanır | sağlanır | N/A |
| FIRST_CHUNK_TIMEOUT | sağlanır | sağlanır | sağlanır | sağlanır | N/A |
| SLOW_CONSUMER | sağlanır | sağlanır | sağlanır | sağlanır | sağlanır |
| RACE_COMPLETION_VS_SHUTDOWN | **ihlal** (2 terminal olay, biri diğerinden sonra) | **ihlal** (lease 2, permit 2) | N/A | sağlanır | N/A |
| RACE_COMPLETION_VS_CLIENT_GONE | sağlanır (yalnızca `complete`) | **ihlal** | N/A | sağlanır | N/A |

### Karar 3: `LeakyStreamHandler` (yalnızca test kodu) davranış tanımı
`final class LeakyStreamHandler implements ClientResponseObserver<GenerateRequest, TokenChunk>`
Constructor: `(GenerateRequest request, SseEmitter emitter, ChannelProvider.Lease lease, PermitLedger permits, int capacity, StreamTimeouts timeouts, UpstreamStarter starter, Duration deadline)`.

Constructor içinde `emitter.onCompletion(this::clientGone)`, `onTimeout(this::clientGone)` ve `onError(e -> clientGone())` kaydedilir. `timer = new StreamTimer(timeouts, this::timedOut)`.

Doğru yaptıkları:
- `start()`: `starter.start(request, deadline, this)`.
- `beforeStart(stream)`: `call = stream`; `stream.disableAutoRequestWithInitial(capacity)`; `timer.start(capacity)`.
- `onNext(chunk)`: terminated ise döner. Değilse `timer.chunkReceived(last)`, `received++`, ardından `sendLock` (`ReentrantLock`, `lock()`) altında terminated tekrar kontrol edilip chunk olayı `SseEvents` formatında senkron yazılır. `IOException`/`IllegalStateException` olursa `clientGone()` çağrılır. Son chunk değilse `creditReturned++`, `timer.creditGranted(1)` ve `call.request(1)`.
- Kredi değişmezi: `received - creditReturned <= capacity`. Ayrı bir kuyruk yok; sınırı manuel flow control sağlıyor.
- `onCompleted()`: `terminal(() -> { timer.close(); release(); send(complete); emitter.complete(); })`.
- `onError(t)`: aynı biçimde `error(Status.fromThrowable(t).getCode().name())` gönderir.
- `cancelForShutdown()`: `terminal(() -> { timer.close(); call.cancel(SHUTDOWN_MESSAGE, null); release(); send(error CANCELLED); complete(); })`.
- `timedOut(kind)`: shutdown ile aynı yolu izler; mesaj `kind.message()`, kod `DEADLINE_EXCEEDED`. Yazma zamanlayıcı thread'inde senkron yapılır. Bu üretimden farklı bir tasarım, ancak sözleşmenin hiçbir maddesinin ölçtüğü bir şey değil; `failure-analysis.md`'de belirtilir.
- `release()`: `lease.close()` ve `permits.release()`.
- Kendi cancel'ından sonra gelen geç `onError` callback'i check'te terminated'i görür ve yok sayılır. Burada sıralama garantili: önce set, sonra cancel, en son callback.

Kusur (a): `clientGone()` şu şekildedir: `terminal(() -> { timer.close(); release(); /* call.cancel YOK */ })`. Kapasite ve zamanlayıcı doğru biçimde serbest kalır; yalnızca upstream generation açık kalır. HANG nedeniyle `active_generations` 1'de kalır ve sonradan hiçbir callback gelmez.

Kusur (b): `private boolean terminal(Runnable act)` metodu `volatile boolean terminated` üzerinde check-then-act yapar: `if (terminated) return false; raceHook(); terminated = true; act.run(); return true;`.

- `raceHook()`: `CountDownLatch l = raceArrivals` (volatile, varsayılan null). null ise hiçbir şey yapmaz; yarış dışı senaryolarda kanca hiç devreye girmez ve terminal yollar sıralı kalır. null değilse `l.countDown(); if (!l.await(RACE_HOOK_WAIT = 5 s)) raceHookBroken = true`. `InterruptedException` gelirse kanca bozuk sayılır ve interrupt bayrağı geri konur. Latch tek kullanımlıktır: count 0'a düştükten sonra gelen geç çağrılar beklemeden geçer, yani başka çağrı asılı kalamaz.
- `armTerminalRace(CountDownLatch arrivals)` yalnızca yarış testinde, `start()`'tan sonra ve tetiklerden önce çağrılır. Erişimci: `raceHookBroken()`.
- Determinizm: kancaya yalnızca check'i geçmiş bir çağrı ulaşabilir. Bir taraf, öteki taraf da check'i geçip latch'e gelene kadar `act` aşamasına geçemez. Dolayısıyla iki taraf da her çalıştırmada check'i geçer. Sonuç bir olasılığa değil, latch'in yapısına bağlıdır.
- Bir taraf hiç gelmezse bekleyen taraf 5 s sonra devam eder ve `raceHookBroken = true` olur. Test bunu ön koşul hatası olarak açık mesajla bildirir; testin asılı kalması yerine kırılması sağlanır.
- `static final class PermitLedger { boolean tryAcquire(); void release(); int releases(); int held(); }` (tek permit).

### REAL için aynı yarış (stres)
REAL'de üretime ayrım noktası eklenmediği için yarış iç kancayla değil test seviyesinde kurulur. A ve B, `Executors.newVirtualThreadPerTaskExecutor()` üzerinde çalışır ve önce test seviyesindeki bir `CyclicBarrier(2)`'de `await(5 s)` ile buluşur, sonra aynı tetikleri çalıştırır.

- İterasyon sayısı N = 200. Her iterasyonda yeni emitter, `CountingLease`, `CountingRegistry` ve `FakeUpstreamStarter` kullanılır; `StreamTimeouts` test başına tektir (30 s).
- İterasyonun durulması `await().pollDelay(ZERO).pollInterval(1 ms).atMost(5 s)` ile beklenir. Koşul: `leaseCloses >= 1 && permitReleases >= 1 && pendingTimers == 0`, shutdown yarışında buna ek olarak `terminal >= 1`.
- Tüm iterasyonlardan sonra bütün anlık görüntüler üzerinde tek bir `awaitStable(QUIET)` çalışır, sonra her iterasyon ayrı değerlendirilir.
- LEAKY aynı döngüyü 5 iterasyonla çalıştırır ve her iterasyonda kancayı kurar. Her iterasyonun kümesi beklenen kümeye eşit olmalı; bu, determinizmin kanıtıdır.
- Kazananların dağılımı yalnızca rapor metnine ve loga yazılır, assert edilmez.
- Neden flaky değil: CAS tasarımı her olası iç içe geçme için değişmezi garanti ediyor. Test hangi tarafın kazandığını assert etmiyor ve süre sınırları ms mertebesindeki işlere karşı 5-10 s. Doğru kodda kırılma olasılığı yok. Kırılırsa bu gerçek bir hata ya da harness hatasıdır.
- Sınır (dokümana da girecek): stres yarışın yokluğunu kanıtlayamaz. Bunun gerekçesi kod okumasıdır (`claim()` CAS). LEAKY'nin deterministik gösterimi, harness'in bu ihlali gördüğünü kanıtlar.
- Beklenen süre: iterasyon başına birkaç ms, yarış başına 2 s'den az.

### Karar 4: Dokümantasyon
- `docs/failure-analysis.md` şunları içerir: iki kusur ve bunları gösteren madde ve senaryolar (yukarıdaki tablo), çalıştırma komutu, beklenen log çıktısı, REAL'in farkları. Farklar: `claim()` CAS ile tek kazanan çıkıyor. Kazanan, canlı bir çağrı varsa önce upstream'i iptal ediyor, sonra `releaseCapacity()` çağırıyor ve bunların hepsi herhangi bir yazmadan önce yapılıyor (`clientGone`, `timedOut`, `cancelForShutdown` ve `overflow` bu sırayı izliyor; `COMPLETED` son chunk yazılmadan önce claim ediliyor). Terminal olay `sendLock` `tryLock` ile yazılıyor. Hipotez olarak işaretlenecek nokta: stresin kapsamı.
- `docs/adr/0002-single-terminal-claim.md` uygun, çünkü kodda gerçekten var olan bir karar ve yeni bir kapsam açmıyor. Başlık: "Single terminal claim by CAS; capacity released before the terminal write". Durum accepted, tarih 2026-10-24. Alternatifler:
  - check-then-act bayrağı (LeakyStreamHandler bunun nasıl bozulduğunu gösteriyor),
  - terminal adımı `sendLock` altında yapmak (takılı bir yazma shutdown'ı ve kapasiteyi bekletir),
  - release'i yazmadan sonra `finish` içinde yapmak (takılı yazma kapasite sızdırır).

  Sonuçlar:
  - client-gone'da terminal olay yazılmaz,
  - COMPLETED claim edildikten sonra shutdown iptal edemez,
  - akış son yazma dönene kadar tracked kalır (README "Known limits").
- `README.md` iki yerden değişir. "Settings" bölümünden önce 3-5 cümlelik `### Stream contract tests` bölümü eklenir ve `docs/failure-analysis.md`'ye bağlanır. "Decisions" listesine ADR-0002 eklenir.

### Kapsamdan çıkarılanlar ve gerekçeleri
- IDLE timeout senaryosu: mevcut `StreamBridgeTimeoutTest` bunu kapsıyor; FIRST_CHUNK zamanlayıcı yolunu temsil etmeye yeter.
- Retry yolu ve overflow: sözleşmenin 5 maddesine yeni bir şey katmıyor.
- Spring/HTTP uçtan uca test: süre ve kapsam nedeniyle dışarıda.
- Per-request mod, loadgen ve MQTT: talep gereği kapsam dışı.

## 3. Uygulama adımları
Tüm test dosyaları `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/` altında.

1. `StreamTestSupport.java` (yalnızca ekleme): `static final class CountingRegistry extends ActiveStreamRegistry` (`AtomicInteger releases`; `@Override void releasePermit(StreamBridge b) { releases.incrementAndGet(); super.releasePermit(b); }`) ve `static CountingRegistry startedCountingRegistry(Duration drainTimeout)`. Constructor `properties(drainTimeout, defaultLimits(), Timeouts.defaults())` ile kurulur, ardından `start()` çağrılır.
2. `StreamContract.java`: `Clause`, `Violation`, `Observation`, `Scenario` (terminal şekli ve ölçülen maddeler), `check(...)`, `EXPECTED_LEAKY`, `expected(Kind, Scenario)`, `isTerminal(String)`, `report(...)`.
3. `LeakyStreamHandler.java`: Karar 3'teki davranış ve `PermitLedger`. Kusurlar dosya içinde `// FLAW (a)` ve `// FLAW (b)` yorumlarıyla işaretlenir.
4. `StreamUnderTest.java`: arayüz, `Kind`, `Wiring`, `create(...)` ile iki nested adaptör: `RealStream` (`CountingRegistry` ve `new StreamBridge(request, emitter, registry, lease, capacity, new StreamMetrics(new SimpleMeterRegistry(), registry), timeouts, RetryPolicy.disabled(), starter, deadline)`) ve `LeakyStream`.
5. `StreamContractTest.java`: `@Timeout(value = 60, threadMode = SEPARATE_THREAD)`. `@BeforeEach` simülatörü, veri kanalını ve admin kanalını açar. `@AfterEach` tüm gate'leri açar (kayıtlı emitter listesi üzerinden), `StreamTimeouts`'u durdurur, veri kanalında `shutdownNow` çağırır (sızan çağrıyı iptal eder), admin kanalını kapatır, `server.stop()` çağırır. Yardımcılar: `awaitQuietly`, `awaitStable`, `runScenario`, `assertMatchesExpectation`. Testler:
   - `@ParameterizedTest @EnumSource(Kind.class)`: `normalCompletionEndsOnce`, `upstreamErrorEndsOnce`, `clientGoneCancelsUpstream`, `shutdownCancelsUpstreamWithinBound`, `firstChunkTimeoutCancelsUpstream`, `slowConsumerStaysWithinQueueBound`.
   - `@Test expectationTableCoversBothFlawsAndOnlyThem`: her `(Kind, Scenario)` çifti için giriş var; REAL'de hepsi boş; LEAKY'de boş olmayan satırlar tam olarak CLIENT_GONE `{UPSTREAM_CANCELLED}`, RACE_VS_SHUTDOWN `{SINGLE_TERMINAL, RELEASE_ONCE}` ve RACE_VS_CLIENT_GONE `{RELEASE_ONCE}`.
6. `TerminalRaceContractTest.java`: aynı `@Timeout`, simülatör yok. `@ParameterizedTest @EnumSource(Kind.class)`: `completionRacesShutdownCancel` ve `completionRacesClientGone`. İterasyon sayısı REAL'de 200, LEAKY'de 5. Executor ve barrier `finally` içinde kapatılır (`shutdownNow`, gerekirse `barrier.reset()`). `StreamTimeouts` `@AfterEach` içinde durdurulur.
7. `docs/failure-analysis.md`, `docs/adr/0002-single-terminal-claim.md` ve `README.md` (iki işaretçi).
8. Kontrolleri çalıştır (bölüm 4). Test sayıları ve süreler implementation.md'ye yazılır.

## 4. Doğrulama

| AC | Kontrol | Beklenen sonuç |
|---|---|---|
| AC1 | `git diff --stat -- gateway/src/main proto simulator` | boş |
| AC2 | `StreamContractTest` içindeki 6 test, REAL invocation'ları | yeşil (küme `{}`) |
| AC3 | `clientGoneCancelsUpstream[LEAKY]` ve diğer LEAKY invocation'ları | yeşil; log'da `UPSTREAM_CANCELLED: active_generations=1 cancelled_total=0` |
| AC4 | `TerminalRaceContractTest[LEAKY]` x2 | yeşil; her iterasyonda beklenen küme; kanca bozulmamış |
| AC5 | `TerminalRaceContractTest[REAL]` x2 | yeşil; 200/200 iterasyon boş küme |
| AC6 | `expectationTableCoversBothFlawsAndOnlyThem` ve küme eşitliği | yeşil |
| AC7 | `Thread.sleep` için `gateway/src` grep'i; dosyaların okunması | eşleşme yok; her sınıfta `@Timeout` |
| AC8 | doküman ve kod karşılaştırması (reviewer) | çelişki yok |
| AC9 | `./mvnw -B verify` | exit 0; simulator 12; gateway 165 + yeni (beklenen 12 + 1 + 4 = 182, gerçek sayı raporlanır) |
| AC10 | surefire raporundaki sınıf süreleri | yaklaşık 40 s'den az |

Komutlar:
- `./mvnw -B verify`
- Odaklı çalıştırma (bayrak implementer tarafından doğrulanacak): `./mvnw -B -pl gateway -am verify -Dtest=StreamContractTest,TerminalRaceContractTest -Dsurefire.failIfNoSpecifiedTests=false`
- Kararlılık kontrolü: odaklı komut art arda 3 kez.

Beklenen süreler: `StreamContractTest` yaklaşık 15 s (12 invocation x 0,5-1,5 s; LEAKY client-gone +2 s); `TerminalRaceContractTest` yaklaşık 6 s.

Riskler:
- R1: Simülatör senaryolarında `RecordingEmitter`'ın sınıf içinde kurulan `onCompletion` override'ı yalnızca son kaydedilen callback'i saklıyor. LEAKY bu callback'i constructor'da kaydetmezse `simulateClientGone()` hiçbir şey yapmaz ve test "beklenen ihlal görülmedi" diye kırılır. Bu davranış istenen yöndedir.
- R2: LEAKY SLOW_CONSUMER'da gRPC callback thread'i gate'te bloklanır. Gate `finally` ve `@AfterEach` içinde açılır. Bloklanma boyunca IDLE zamanlayıcısı ateşlenmemeli; 30 s zamanlayıcı ile bu sağlanır.
- R3: In-process `isReady` ile kredi eşleşmesi yalnızca durmuş istemci rejiminde doğrulandı. Bu yüzden örnekleme yalnızca o pencerede yapılıyor.
- R4: 200 iterasyonda 1 ms poll CPU harcar. Süre aşılırsa N 100'e düşürülür ve gerekçesi raporlanır.
- R5: Doküman ile kod arasındaki tutarsızlık geçmiş review'larda tekrar tekrar bulundu. Her iddia dosya ve metoda bağlanmalı.

Doğrulanmamış varsayımlar (implementer kod ve jar'lara karşı kontrol etmeli):
- A1: Package-private `releasePermit`'in aynı paketteki bir test alt sınıfından override edilebildiği (Java kuralı gereği evet) ve `StreamBridge`'in bunu dinamik dispatch ile çağırdığı.
- A2: `SseEmitter.event().name(..).build()` metninde `event:complete` ve `event:error` önekleri bulunduğu (`UpstreamFlowControlTest` `event:complete` için bunu doğruluyor).
- A3: Kanalda `shutdownNow` sonrası simülatörün `onCancel`'ı tetiklemesi ve `server.stop()`'un 2 s grace süresini beklemeden kapanması.
- A4: Awaitility'nin `during` ile birlikte `until(callable)` kullanımındaki semantiği ve `ConditionTimeoutException` paketi (`org.awaitility.core`).
- A5: Surefire 3.5.4'te `-Dsurefire.failIfNoSpecifiedTests=false` bayrağının adı.

Açık soru yok.
