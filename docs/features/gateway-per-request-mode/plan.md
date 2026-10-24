# Plan: gateway-per-request-mode

Doğrulanmış olgular (HEAD 9136fa4, kod doğrudan okundu, MCP code-graph yoktu):

- **Seam zaten var.** `upstream/ChannelProvider` (`Lease acquire()`, `state(boolean)`, `Lease extends AutoCloseable { Channel channel(); void close(); }`) mevcut. `SharedChannelProvider`, `SmartLifecycle` faz `LifecyclePhases.UPSTREAM_CHANNEL` üzerinde çalışıyor. `start()` içinde `factory.create(target)` çağrılıyor ve `watch` ile bağlantı sıcak tutuluyor. `stop()` sırası: `shutdown()`, ardından `awaitTermination(channelTerminationTimeout)`, gerekirse `shutdownNow()` ve 2 s bekleme. `acquire()` no-op `SharedLease` döner, çalışmıyorsa `IllegalStateException` fırlatır. `isTerminated()` public.
- `UpstreamConfiguration` `nettyChannelFactory` (`NettyChannelBuilder...usePlaintext().build()`), `SharedChannelProvider` (koşulsuz) ve `UpstreamChannelHealthIndicator(ChannelProvider)` bean'lerini tanımlıyor. `ManagedChannelFactory` tek metotlu bir arayüz (`create(String target)`).
- `GatewayProperties.connectionMode`: `@DefaultValue("shared")`, enum içinde yalnızca `SHARED` var. `application.yaml`'da `lab.connection-mode: shared` yazıyor. `ConnectionModeConfigTest#perRequestModeFailsAtStartup` şu an `per-request` değerinin reddedildiğini assert ediyor; bu test değişmek zorunda.
- `StreamController`, `channels.acquire()` çağrısını admission'dan **önce** yapıyor. Upstream starter lambda'sı `TokenStreamServiceGrpc.newStub(lease.channel())` kullanıyor. Admission reddedilirse `lease.close()` çağrılıyor. `bridge.start()`, `registry.admit()` döndükten sonra request thread'inde çalışıyor; `admit` lock'u `finally` içinde bırakıyor.
- `StreamBridge.startCall` → `starter.start(...)` kilitsiz çalışıyor: ilk çağrı `start()` üzerinden, retry'lar `stream-retry` virtual thread'inde `startAttempt` üzerinden. `creditLock` ancak `beforeStart` içinde, yani `lease.channel()` döndükten sonra alınıyor. Starter bir `RuntimeException` fırlatırsa çağrı `attempt.fail(INTERNAL "failed to start upstream call")` olur. INTERNAL retry edilmiyor (`RetryPolicy.isRetryable` yalnızca UNAVAILABLE için true).
- `lease.close()` yalnızca `releaseCapacity()` içinden çağrılıyor ve bunu yalnızca claim kazananı yapıyor. Bazı yollarda **`sendLock` tutulurken** çağrılıyor: `writeChunkLocked` içindeki COMPLETED claim'i ve ara chunk yazımı başarısız olunca `clientGone()`. Öteki yollar: `handleUpstreamEnd` (drainer, kilit yok), `overflow` (gRPC thread'i), `timedOut` (scheduler), `cancelForShutdown` (registry `stop()` → `cancelAll`; snapshot registry lock'u bırakıldıktan sonra işleniyor). Cancel yollarında `cancelUpstream` her zaman `releaseCapacity`'den önce. Son chunk'ın yazımı başarısız olursa yalnızca `terminal.set(CLIENT_GONE)` yapılıyor; ikinci bir release yok. Retry backoff'u sırasında lease tutulmaya devam ediyor.
- `ActiveStreamRegistry.stop()` faz `STREAM_ADMISSION` üzerinde, kanal fazından önce duruyor: drain, ardından force-cancel. Final yazımı takılı kalan akışların lease'i zaten bırakılmış.
- `StreamMetrics` constructor'ı `(MeterRegistry, ActiveStreamRegistry)` ve testlerde bu iki argümanla çok yerde kullanılıyor; değiştirilmemeli. Tag stili alt çizgili: `first_chunk`, `queue_timeout`.
- Test altyapısı:
  - `InProcessSimulatorConfig`: `@Primary ManagedChannelFactory` her çağrıda `InProcessChannelBuilder.forName(name).intercept(faults).build()` döndürüyor, yani per-request kanallarına da fault interceptor uygulanıyor.
  - `StreamUnderTest.Wiring` somut `CountingLease` istiyor ve `CountingLease.channel()` exception fırlatıyor.
  - `StreamContractTest.runAndAssert` private; testin kendi `channel` alanını kullanıyor ve contract'ı REAL + gerçek in-process simulator ile 6 senaryoda koşuyor.
  - Race testleri `FakeUpstreamStarter` kullanıyor; gerçek kanal yok.
- Repoda log yakalayan test yok (`CapturedOutput` veya `ListAppender` bulunamadı) ve thread sayan test yok.

## 1. Davranış ve kabul kriterleri

`lab.connection-mode=per-request` modunda her akış kendi `ManagedChannel`'ını kullanır. Kanal **lazy** açılır: lease'in ilk `channel()` çağrısında, yani ilk upstream çağrısı başlarken. Bu an admission'dan ve permit alındıktan sonradır ve hiçbir kilit tutulmuyordur. Kanal akışın tek terminal claim'inde mevcut `releaseCapacity()` → `lease.close()` yolu üzerinden bir kez, non-blocking ve graceful `shutdown()` ile kapanır. Varsayılan mod `shared` olarak kalır ve davranışı değişmez. `StreamBridge`, `StreamController`, `RetryPolicy`, `StreamContract`, proto ve simulator değişmez.

**Retry kararı: aynı kanal yeniden kullanılır.** Gerekçeler:
- Mevcut tasarımda lease akış boyunca yaşıyor (README "While waiting... keeps its permit and lease"). Böylece kapanış tek release yolunda kalır ve "akış başına tam bir kez" doğrudan claim CAS'ından gelir.
- Deneme başına kanal açmak, `attemptEnded`/`retryAfter` içine ikinci bir kapanış yolu eklemeyi gerektirirdi; bu `StreamBridge`'i değiştirmek demek.
- O durumda retry semantiği de shared moddan ayrılırdı: taze kanal, kanalın kendi connect backoff'unu atlar. Sonraki karşılaştırmada fark yalnızca "akış başına kanal" maliyeti olmalı.

- **AC1 (konfigürasyon):**
  - Varsayılan `SHARED`.
  - `per-request`, `per_request` ve `PER_REQUEST` değerleri `ConnectionMode.PER_REQUEST`'e bağlanır (Boot lenient enum).
  - `lab.connection-mode=pooled` ve boş değer startup'ta hata verir; stack trace `lab.connection-mode` içerir.
  - Context'te tam bir `ChannelProvider` bean'i olur: shared modda `SharedChannelProvider`, per-request modda `PerRequestChannelProvider`.
- **AC2 (oluşturma noktası):**
  - Per-request kanal yalnızca `lease.channel()` ilk çağrıldığında oluşturulur. Bu çağrı `sendLock`, `creditLock` veya registry lock'u altında yapılmaz.
  - Reddedilen (503) ve WAIT kuyruğunda bekleyen isteklerde kanal açılmaz (`created` artmaz).
  - Retry/resume yapan bir akış toplamda 1 kanal açar.
- **AC3 (tam bir kez kapanış):**
  - Açılan her per-request kanal tam bir `shutdown()` alır ve normal yollarda `shutdownNow()` almaz. `lease.close()` idempotent ve non-blocking'dir (CAS).
  - Kapanış tüm terminal yollarda olur: completion, upstream error, client gone, timeout, overflow, shutdown cancel ve retry sonrası bitiş.
  - Shared modda akışlar kanalı hiç kapatmaz (`shutdown` 0); kanal yalnızca provider `stop()` sırasında 1 kez kapanır.
- **AC4 (contract iki modda):**
  - `StreamContractTest`'e eklenen `contractHoldsInEveryConnectionMode(ConnectionMode, Scenario)` testi, REAL ve gerçek in-process simulator ile `COMPLETION`, `UPSTREAM_ERROR`, `CLIENT_GONE`, `SHUTDOWN_CANCEL`, `FIRST_CHUNK_TIMEOUT` ve `SLOW_CONSUMER` senaryolarını her iki modda koşar. Her biri boş ihlal kümesi verir; `active_generations` değerinin 0'a dönmesi contract'ın UPSTREAM_CANCELLED maddesinin parçası.
  - Ek gözlem olarak per-request'te `created == 1`, kanalın `shutdown == 1` ve `shutdownNow == 0` değerleri ve sınırlı süre içinde `isTerminated()` beklenir.
  - Shared'de `created == 1` ve provider `stop()` öncesi `shutdown == 0` beklenir.
  - Mevcut `Kind` testleri ve harness testleri değişmeden yeşil kalır.
- **AC5 (uçtan uca, iki mod):** `ConnectionModeEndToEndTest` her mod için şunları doğrular:
  - (a) Ardışık 3 akış: per-request'te `created == 3` ve her kanal 1 kez kapanıp sonlanmış; shared'de `created == 1` ve `shutdown == 0`.
  - (b) `faults.cutNextCalls(2, 3)` ile resume: 10 chunk ve `complete`, `retries_total{resume} == 2`; per-request'te `created == 1` ve `shutdown == 1`.
  - (c) `max-active-streams=1`: ikinci istek 503 alır ve `created` artmaz.
  - (d) Yalnızca per-request: kanal oluşturma hatası enjekte edilir. Tek `error` olayı gelir (`"code":"INTERNAL"`, `failed to start upstream call`), `active_streams == 0`, simulator `started_total` değişmez ve bir sonraki akış normal tamamlanır.
- **AC6 (shutdown):**
  - Per-request modda HANG akışı açıkken context kapatılır. Akış tek `error` `CANCELLED` alır.
  - `close()` en geç drain + 2 s + `channel-termination-timeout` + 2 s içinde döner.
  - `close()` döndükten sonra counting factory'nin açtığı tüm kanallar `isTerminated()` durumundadır, `PerRequestChannelProvider.openChannels() == 0` olur, hiçbir kanal ikinci kez kapatılmamıştır.
  - Provider ve lease thread veya executor oluşturmaz (kaynak incelemesiyle doğrulanır).
- **AC7 (gözlemlenebilirlik):**
  - `connection_mode{mode="shared"|"per_request"}` gauge'u değeri 1 ile, tam bir meter olarak kayıtlıdır ve `/actuator/metrics/connection_mode` 200 döner.
  - Startup'ta tek bir INFO satırı modu ve hedefi yazar.
  - Metric veya loglarda request id ya da kanal başına tag olmaz; kanal başına INFO log da olmaz.
- **AC8 (lease yarışları):** `PerRequestChannelProviderTest` sınırlı iterasyonla `close`↔`close`, `close`↔`channel()` ve `stop`↔`channel()` yarışlarını koşar. Her iterasyonda açılan her kanal tam bir kapanış alır (`shutdown` + `shutdownNow` == 1) ve kapandıktan sonra `channel()` hiçbir zaman açık bir kanal döndürmez.
- **AC9 (dokümanlar):**
  - README'ye "Connection modes" bölümü eklenir; ölçülmeyen şeyler açıkça yazılır, performans iddiası olmaz.
  - Settings ve Metrics tabloları ile iki "per-request mode (not implemented)" known-limit maddesi güncellenir.
  - `docs/adr/0003-per-request-connection-mode.md` eklenir; `docs/failure-analysis.md`'ye kısa not düşülür.
- **AC10 (kapsam ve hijyen):**
  - `git diff -- proto simulator gateway/src/main/java/io/github/cemharput/streamlab/gateway/stream` boş kalır.
  - `StreamContract.java`, `StreamUnderTest.java`, `TerminalRaceContractTest.java`, `TerminalPathRaceTest.java`, `LeakyStreamHandler.java`, `GracefulShutdownTest.java`, `ReadinessDownTest.java`, `RetryAndResumeTest.java` ve `StreamEndToEndTest.java` değişmez.
  - Değişen testler yalnızca bölüm 3'te listelenenler; `ConnectionModeConfigTest`'te yalnızca `perRequestModeFailsAtStartup` değiştirilir.
  - `Thread.sleep` kullanılmaz. Yeni sınıflarda `@Timeout(value = 60, threadMode = SEPARATE_THREAD)` bulunur. Beklemeler sınırlıdır ve en az 5 kat pay taşır. Gate'ler `finally` veya `@AfterEach` içinde açılır.
- **AC11:** `./mvnw -B verify` art arda 3 kez yeşil geçer.
- **AC12:** Mevcut ürün kodunda (`StreamBridge`, registry, controller) gerçek bir defekt bulunursa iş durur ve kanıtıyla raporlanır. Feature `blocked` olur, assert zayıflatılmaz, düzeltme ayrı bir feature'a bırakılır. Yeni koddaki (provider, config) hatalar bu feature içinde düzeltilir.

## 2. Dosyalar, semboller ve tasarım

### Main (`gateway/src/main/java/io/github/cemharput/streamlab/gateway/`)

**`config/GatewayProperties.java`**
- `enum ConnectionMode { SHARED, PER_REQUEST; String tag() }`; tag değerleri `"shared"` ve `"per_request"`.
- Compact constructor'a `connectionMode == null` kontrolü eklenir; mesaj: `lab.connection-mode must be shared or per-request` (boş değer bu kontrole düşer).
- "Only the shared mode exists" javadoc'u güncellenir.

**`upstream/PerRequestChannelProvider.java` (yeni, public, `ChannelProvider` + `SmartLifecycle`, faz `UPSTREAM_CHANNEL`)**

Alanlar:
- `ManagedChannelFactory factory`
- `GatewayProperties properties`
- `volatile boolean running`
- `Set<PerRequestLease> open` (`ConcurrentHashMap.newKeySet()`): kanalı oluşmuş ama henüz kapanmamış lease'ler.
- `Set<ManagedChannel> closing`: `shutdown()` çağrılmış ama sonlanması henüz görülmemiş kanallar.

Metotlar:
- `start()`: `running = true`, ardından tek INFO satırı: `Upstream connection mode per-request: one channel per stream to {target}`.
- `acquire()`: `!running` ise `IllegalStateException` fırlatır (controller bunu 503 `upstream_unavailable` yapar). Aksi halde yeni bir `PerRequestLease` döner; bu aşamada kanal oluşmaz ve set'e ekleme yapılmaz.
- `state(rc)`: `running ? READY : SHUTDOWN`. Uzun ömürlü bir kanal olmadığı için readiness yalnızca lifecycle'ı yansıtır (karar, bkz. açık soru S1).
- `stop()`:
  1. `running = false`.
  2. `open` snapshot'ındaki her lease için `lease.close()` (backstop; normalde boştur). Boş değilse sayıyla tek WARN logu yazılır.
  3. `closing` içindeki kanallar tek bir `channelTerminationTimeout` deadline'ı ile beklenir.
  4. Kalanlara `shutdownNow()` yapılıp 2 s beklenir; hâlâ sonlanmayanlar için sayıyla ERROR logu yazılır. Interrupt durumunda `shutdownNow()` çağrılır ve interrupt flag'i yeniden set edilir; desen `SharedChannelProvider.stop()` ile aynı.
- Test erişimcileri: `openChannels()` (`open.size()`) ve `isTerminated()` (`!running && open.isEmpty() && closing` içindekilerin hepsi sonlanmış).

İç sınıf `PerRequestLease`, `AtomicReference<State>` kullanır. Durumlar: `EMPTY`, `OPEN(ch)`, `CLOSED`.
- `channel()` bir döngüdür:
  - `OPEN` ise kanalı döner.
  - `CLOSED` ise ya da provider çalışmıyorsa `IllegalStateException` fırlatır.
  - `EMPTY` ise:
    1. Önce `closing.removeIf(ManagedChannel::isTerminated)` ile budama yapılır (burada, close yolunda değil).
    2. `fresh = factory.create(target)`; **kilit tutulmaz**.
    3. `CAS(EMPTY → OPEN(fresh))` kazanılırsa `open.add(this)`. Ardından `!running` ise `close()` çağrılır ve `IllegalStateException` fırlatılır (stop yarışı), değilse `fresh` döner.
    4. CAS kaybedilirse `fresh.shutdownNow()` çağrılır (hiç kullanılmamış kanal) ve döngü yeniden okunur.
  - Factory exception fırlatırsa exception olduğu gibi yayılır ve durum `EMPTY` kalır.
- `close()`: `CAS(s → CLOSED)`. Kazanan taraf, önceki durum `OPEN` ise `open.remove(this)`, `closing.add(ch)` ve `ch.shutdown()` çağırır. `shutdown()` non-blocking ve graceful; `awaitTermination` burada **çağrılmaz**, çünkü bu yol `sendLock` altında da çalışıyor. Kaybeden veya `EMPTY`'den gelen çağrı hiçbir şey yapmaz.

**Neden `shutdown()`, `shutdownNow()` değil:**
- Cancel yollarında çağrı zaten `cancelUpstream` ile iptal edilmiş oluyor.
- COMPLETED claim'i son chunk yazılırken yapılıyor ve o anda trailer'lar henüz gelmemiş olabilir. `shutdownNow()` bitmek üzere olan çağrıyı iptal eder; simulator'ın `completed_total`/`cancelled_total` sayaçları ve contract bozulabilir.
- Claim'den sonra başlamaya çalışan bir retry çağrısı ya `channel()` üzerinde `IllegalStateException` alır (`startCall` bunu yakalar, terminal zaten set) ya da `attemptBeforeStart`'ın "publish then check terminal" adımında iptal edilir. Bu davranış mevcut koddan doğrulandı. Böylece graceful kapanış takılmaz.

**Terminal yollar ve kapanış:**

| Yol | Kim kapatır | Thread / kilit |
|---|---|---|
| COMPLETED | `writeChunkLocked` → `releaseCapacity` | drainer, `sendLock` altında (close non-blocking) |
| UPSTREAM_ERROR | `handleUpstreamEnd` | drainer, kilit yok |
| Overflow | `overflow` | gRPC callback thread'i |
| CLIENT_GONE | `clientGone` | emitter callback veya `writeChunkLocked` (`sendLock`) |
| Timeout | `timedOut` | scheduler, kilit yok |
| SHUTDOWN_CANCELLED | `cancelForShutdown` | registry stop thread'i, registry lock'u bırakılmış |
| Retry | ara kapanış yok; kanal korunur; son bitiş yukarıdaki yollardan biridir | — |
| Start veya oluşturma hatası | UPSTREAM_ERROR yolu (INTERNAL) | kanal oluşmadıysa close no-op |
| Admission reddi | `StreamController` `lease.close()` | kanal hiç oluşmadığı için no-op |
| Provider stop | backstop `lease.close()` | CAS sayesinde tam bir kez |

**`upstream/UpstreamConfiguration.java`**
- `@Bean @Conditional(SharedMode.class) SharedChannelProvider` (dönüş tipi aynı kalır, `GracefulShutdownTest`'in `getBean(SharedChannelProvider.class)` çağrısı çalışmaya devam eder).
- `@Bean @Conditional(PerRequestMode.class) PerRequestChannelProvider`.
- Nested `Condition` sınıfları modu `Binder.get(context.getEnvironment()).bind("lab.connection-mode", ConnectionMode.class).orElse(SHARED)` ile okur. Bu, properties bağlamasıyla aynı lenient dönüşümü kullanır. Geçersiz bir değerde condition veya properties bağlaması startup'ı düşürür.
- Neden switch'li tek bir `ChannelProvider` bean'i değil: lifecycle ve `getBean` tip eşleşmesi bildirilen dönüş tipine bağlı olabilir; bu Boot 4'te doğrulanmadı.

**`upstream/ConnectionModeMetrics.java` (yeni `@Component`)**
- Constructor: `(MeterRegistry, GatewayProperties)`.
- `Gauge.builder("connection_mode", () -> 1).tag("mode", mode.tag())` kaydeder.
- Root package taraması `upstream` paketini kapsıyor. `StreamMetrics`'e dokunulmaz.

**`upstream/SharedChannelProvider.java`**
- Yalnızca `start()` içine tek INFO satırı eklenir: `Upstream connection mode shared: one channel to {target}`. Başka değişiklik yok.

**`upstream/ChannelProvider.java`**
- Yalnızca javadoc: lease bir akışa aittir; `channel()` kanalı lazy oluşturabilir ve aynı lease'te aynı kanalı döner; `close()` idempotent, non-blocking ve terminal claim kazananı tarafından çağrılır.

**`application.yaml`**
- Değer `shared` kalır; `# shared | per-request` yorumu eklenir.

`StreamController` ve `StreamBridge` **değişmez**: lease admission'dan önce alınıyor, ama kanal artık admission'dan sonra lazy oluşuyor. Bu, README'deki "lease would have to be acquired after admission" notunun gerektirdiği şeyi karşılıyor.

### Test

- **`testsupport/CountingChannelFactory.java` (yeni, public)**
  - Bir delegate `ManagedChannelFactory`'yi sarar ve her kanalı `CountingChannel extends ManagedChannel` içine koyar. `CountingChannel` 11 metodun hepsini delege eder ve `shutdown`/`shutdownNow` çağrılarını sayar.
  - Erişimciler: `created()`, `channels()`, `failNextCreations(int)`.
  - Enjekte edilen hata `IllegalStateException("injected channel creation failure")` olarak fırlatılır ve sayılır.
- **`testsupport/InProcessSimulatorConfig.java` (eklemeli)**
  - Mevcut lambda bir `CountingChannelFactory` alanıyla sarılır ve `channelFactory()` erişimcisi eklenir. Mevcut testler için sarmalama şeffaf.
- **`stream/StreamTestSupport.java` (eklemeli)**
  - `CountingLease(ChannelProvider.Lease delegate)` constructor'ı eklenir: `channel()` delegate'e gider, `close()` önce sayar sonra delege eder. Argümansız constructor davranışını korur.
  - `properties(ConnectionMode, Duration, Limits, Timeouts)` overload'ı eklenir.
- **`stream/StreamContractTest.java` (eklemeli)**
  - `setUp` server adını bir alanda saklar.
  - Mevcut `runAndAssert(kind, scenario)`, lease ve starter'ı oluşturup yeni `runAndAssert(kind, scenario, CountingLease, UpstreamStarter)` metoduna delege eder; gövde aynı kalır.
  - Yeni `@ParameterizedTest @MethodSource("modesAndScenarios")` testi şöyle çalışır:
    1. `CountingChannelFactory(target -> InProcessChannelBuilder.forName(serverName).build())` oluşturulur.
    2. Moda göre provider kurulur ve `start()` edilir.
    3. `lease = new CountingLease(provider.acquire())`; starter `newStub(lease.channel())` kullanır.
    4. Contract koşulur, ardından moda göre kanal assert'leri yapılır.
    5. `finally` içinde `provider.stop()` çağrılır.
    6. Stop sonrasında tüm kanalların `isTerminated()` olması 10 s'ye kadar beklenir; per-request'te `openChannels() == 0`.
- **`gateway/upstream/PerRequestChannelProviderTest.java` (yeni)**
  - Factory: var olmayan bir ada `InProcessChannelBuilder`.
  - Tekil durumlar:
    - Start öncesi ve stop sonrası `acquire` exception fırlatır.
    - `channel()` çağrılmadan `close` → `created == 0`.
    - İki `channel()` çağrısı aynı örneği döner.
    - Çift `close` → `shutdown == 1`.
    - Close sonrası `channel()` exception fırlatır.
    - Factory hatası yayılır ve `openChannels() == 0`.
    - Kapanmamış lease ile `stop` → backstop tek kapanış yapar, sonraki `close` no-op.
  - Yarışlar `TerminalPathRaceTest` iskeletiyle kurulur: `CyclicBarrier(2)`, virtual-thread executor, `onSpinWait` sweep, iterasyon başına 300–500 tekrar, 5 s barrier/future timeout, `finally` içinde `shutdownNow` ve `awaitTermination`.
- **`gateway/ConnectionModeEndToEndTest.java` (yeni)**
  - `SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class)` ile `--lab.connection-mode=<mode>` verilir; `@ParameterizedTest @EnumSource(ConnectionMode.class)`.
  - AC5 (a)–(d), AC6 ve AC7'yi kapsar.
  - Shutdown testinde `drain-timeout=1s` kullanılır ve `close` süresinin 10 s altında kalması assert edilir.
- **`gateway/ConnectionModeConfigTest.java`**
  - `perRequestModeFailsAtStartup` testi `perRequestModeIsBoundFromRelaxedNames` (3 yazım + bean tipi) ve `invalidConnectionModeFailsAtStartup` (`pooled`, boş) testleriyle değiştirilir.

`StreamContract` maddeleri değişmez: kanal sayıları test metotlarında ek gözlem olarak assert edilir.

## 3. Uygulama adımları

1. `GatewayProperties`: `PER_REQUEST`, `tag()` ve null kontrolü. Ardından `ConnectionModeConfigTest` güncellenir.
2. `PerRequestChannelProvider` ve `PerRequestChannelProviderTest` (önce birim testler, sonra yarış testleri).
3. `UpstreamConfiguration` koşulları, `ConnectionModeMetrics`, `SharedChannelProvider` log satırı, `ChannelProvider` javadoc'u ve `application.yaml` yorumu.
4. Test altyapısı: `CountingChannelFactory`, `InProcessSimulatorConfig` sarmalaması, `StreamTestSupport` eklemeleri.
5. `StreamContractTest` eklemeli overload ve mod testi.
6. `ConnectionModeEndToEndTest`.
7. Dokümanlar:
   - `README.md`: "Connection modes" bölümü. Shared modda tek, sıcak tutulan kanal; per-request modda akış başına lazy kanal, ilk çağrıda bağlantı kurulur, terminalde graceful kapanır, retry'lar aynı kanalı kullanır. Readiness per-request modda yalnızca lifecycle'ı yansıtır.
   - "**Not measured yet**" listesi: gecikme, throughput, CPU/bellek, bağlantı kurma maliyeti, gerçek Netty/TCP/HTTP2, TLS, event-loop paylaşımı, port/TIME_WAIT, gerçek ağ kopmasında connect-backoff etkisi. Testler in-process çalışır; performans iddiası yoktur.
   - Settings satırı: `shared` (`shared` veya `per-request`). Metrics satırına `connection_mode{mode}` eklenir. 277–278 ve 363–364. satırlardaki known-limit maddeleri gerçekleşen davranışla değiştirilir. Durum satırı güncellenir.
   - `docs/adr/0003-per-request-connection-mode.md`: lazy lease kanalı, claim'de non-blocking graceful kapanış, retry'da yeniden kullanım; alternatifler (eager acquire, deneme başına kanal, `shutdownNow`, havuz).
   - `docs/failure-analysis.md`: contract'ın artık iki modda koştuğunu söyleyen kısa not.
8. Odaklı komutlar, ardından 3 kez tam verify.

## 4. Doğrulama

| AC | Kontrol | Beklenen |
|---|---|---|
| AC1 | `ConnectionModeConfigTest` | yazımlar bağlanır, bean tipi doğru; geçersiz değer startup hatası, mesajda `lab.connection-mode` |
| AC2 | E2E (c), contract mod testi, kaynak incelemesi | rejected'da `created` sabit; ilk `channel()` kilitsiz; resume'da `created == 1` |
| AC3 | contract mod testi + E2E (a)/(b) + birim testler | her per-request kanalda `shutdown == 1`, `shutdownNow == 0`; shared'de stop öncesi 0 |
| AC4 | `StreamContractTest#contractHoldsInEveryConnectionMode` (12 vaka) + mevcut testler | ihlal yok; `active_generations == 0` |
| AC5 | `ConnectionModeEndToEndTest` | (a)–(d) yukarıdaki gibi |
| AC6 | E2E shutdown testi | `CANCELLED`, close 10 s altında, tüm kanallar sonlanmış, `openChannels() == 0` |
| AC7 | E2E metric testi | tam bir `connection_mode` meter'ı, doğru tag, actuator 200 |
| AC8 | `PerRequestChannelProviderTest` yarışları | uyumsuzluk listesi boş |
| AC9 | Doküman incelemesi | iddialar kodla çelişmez, performans iddiası yok |
| AC10 | `git diff --stat -- proto simulator gateway/src/main/java/io/github/cemharput/streamlab/gateway/stream` + listedeki testler | boş |
| AC11 | 3× tam verify | exit 0, flaky yok |

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- `./mvnw -B verify` (3 kez)
- `./mvnw -B -pl gateway -am verify -Dtest=PerRequestChannelProviderTest,ConnectionModeConfigTest,StreamContractTest,ConnectionModeEndToEndTest -Dsurefire.failIfNoSpecifiedTests=false`

**Riskler:**
- **Kilit altında kapanış:** COMPLETED ve yazma-hatası yollarında `close` `sendLock` altında çalışıyor. Bu yüzden yalnızca non-blocking `shutdown()` kullanılır; `awaitTermination` yalnızca `stop()` içinde. Budama `channel()` içinde yapılır.
- **Çift kapanış:** lease CAS'ı, claim CAS'ı (ADR-0002) ve stop backstop'unun lease üzerinden gitmesi bunu önler. Bunlar yarış testleriyle ölçülür, ama kanıt değildir.
- **Leak:** CAS yarışında `closing` set'ine bir `stop` sonrası eklenen kanal await'i kaçırabilir; kanal yine de kapatılmış olur. Bu durum ADR'de belirtilir.
- **In-process ve gerçek Netty farkı:** in-process kanalın thread'i yok, bağlantı kurma maliyeti yok ve connect-backoff'u yok. Gerçek ağda UNAVAILABLE sonrası per-request kanal da TRANSIENT_FAILURE'a girer ve retry hızlıca tükenebilir; bu shared modla aynı davranış ama test edilmez. Netty'nin paylaşılan event loop'u son kanal sonlandıktan yaklaşık 1 s sonra serbest kalır; bu da test edilmez.
- **"Thread kalmaz" kanıtı:** provider thread oluşturmaz ve kanal sonlanması gRPC kaynaklarının bırakılma koşuludur. Thread adına dayalı katı bir assert JVM paylaşımı yüzünden flaky olur, bu yüzden eklenmez (S2).
- **Süre:** `StreamContractTest` yaklaşık +15–25 s, E2E yaklaşık +20 s.
- **Boot 4 condition binder:** `Binder` ile lenient enum dönüşümü varsayıldı. Varsayım yanlışsa adım 3'te tek bean + switch alternatifine geçilir; `GracefulShutdownTest` bunu yakalar.

**Varsayımlar:**
- `ManagedChannel.shutdown()` dokümana göre non-blocking ve idempotent.
- `NettyChannelBuilder.build()` bağlantı açmaz.
- Startup log satırı test edilmez; repoda log yakalama deseni yok. Metric gauge'u gözlemlenebilirlik kanıtıdır.

**Açık sorular (bloklayıcı değil; varsayılan kararlar alındı):**
- **S1:** Per-request modda readiness yalnızca lifecycle'ı yansıtır. Alternatif, yalnızca health için ayrı bir probe kanalı; bu da karşılaştırmaya bir boşta bekleyen bağlantı ekler. Kullanıcı farklısını isterse yalnızca `PerRequestChannelProvider.state` değişir.
- **S2:** "Thread kalmaz" kanal sonlanması ve kaynak incelemesiyle kanıtlanıyor. Katı bir thread assert'i istenirse ek bir flakiness riski olarak ele alınmalı.
