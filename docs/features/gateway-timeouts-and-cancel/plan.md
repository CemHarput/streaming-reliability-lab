# Plan: gateway-timeouts-and-cancel

## 1. Davranış ve kabul kriterleri

Gateway iki zamanlayıcı kazanır: ilk chunk zaman aşımı (`lab.timeouts.first-chunk`) ve chunk'lar arası sessizlik zaman aşımı (`lab.timeouts.idle-between-chunks`). İkisi de yalnızca **upstream sessizliğini** ölçer, yavaş istemciyi ölçmez. Zamanlayıcı dolunca akış mevcut CAS claim yoluyla biter: upstream iptal edilir, `releaseCapacity()` tam bir kez çağrılır, istemciye tek bir `error` gider (`code=DEADLINE_EXCEEDED`). Yazım mevcut `sendLock`/`tryLock(SEND_LOCK_WAIT)` yolundan yapılır. Ayrıca istemci kopması, timeout ve shutdown iptallerinin upstream'e ulaştığı simulator'ın `active_generations` sayacıyla kanıtlanır. `proto/` ve `simulator/` değişmez.

- **AC1 (ilk chunk timeout):** `lab.timeouts.first-chunk=T` iken upstream çağrısı başladıktan sonra T içinde hiç chunk gelmezse istemci yalnızca şu olayı alır: `error` olayı, `"code":"DEADLINE_EXCEEDED"`, `"message":"first chunk timeout"`. Hiç `chunk` olayı yoktur ve error'dan sonra hiçbir şey gelmez. Upstream `"first chunk timeout"` mesajıyla iptal edilir, `timeouts_total{kind="first_chunk"}` 1 artar. Geçen süre ≥ T olur. `max-active-streams=1` iken sonraki akış kabul edilir ve `complete` ile biter. Bu davranış HANG@0 ve yavaş ilk chunk (`firstDelayMs` > T) ile gösterilir.
- **AC2 (idle timeout):** `lab.timeouts.idle-between-chunks=I` iken gateway'in upstream'e verdiği kredi > 0 ve upstream I boyunca chunk göndermezse şu olur: o ana kadar alınan chunk'lar sırayla yazılır, ardından tek `error` gelir (`DEADLINE_EXCEEDED`, `"idle timeout between chunks"`). Upstream iptal edilir ve `timeouts_total{kind="idle"}` 1 artar. Bu davranış HANG@3 ve yavaş aralık (`intervalMs` > I) ile gösterilir.
- **AC3 (idle semantiği):**
  - (a) Upstream'den **alınan** her chunk idle zamanlayıcıyı yeniden başlatır. Toplam süre I'yı aşsa bile I'dan kısa aralıklarla gelen chunk'lar timeout üretmez.
  - (b) Kredi 0 iken, yani manuel akış kontrolü upstream'i durdurmuşken (istemci yavaş, kuyruk ve yazım dolu), idle zamanlayıcı kurulu değildir ve ateşlenmez. Kredi geri verilince (`request(1)`) yeniden kurulur.
  - (c) `last` chunk alındıktan sonra ya da upstream sonu (`onError`/`onCompleted`) kaydedildikten sonra hiçbir zamanlayıcı kurulmaz.
  - (d) İlk chunk zamanlayıcısı kabul (admission) beklemesini saymaz.
- **AC4 (iptalin upstream'e ulaştığının kanıtı):** Dört neden için, istemci kopması, ilk chunk timeout'u, idle timeout ve shutdown iptali, `SimulatorAdminService.GetStats.active_generations` zaman sınırlı bir Awaitility beklemesiyle 0'a iner ve `cancelled_total` 1 artar. Negatif kontrol: iptal edilmeyen bir HANG akışında `active_generations` ≥2 s boyunca 1 kalır.
- **AC5 (tek terminal, tek bırakma):** Bölüm 2'deki etkileşim matrisinin her satırında tam bir terminal sonuç oluşur, en fazla bir terminal olay yazılır ve `CountingLease.closes == 1` olur. Zamanlayıcının ateşlenmesi takılı bir istemci yazımını beklemez: upstream iptali, permit ve lease bırakma yazım takılıyken de olur. Takılı yazımlar, başka akışların zamanlayıcılarının ateşlenmesini de geciktirmez.
- **AC6 (zamanlayıcı sızıntısı yok, yaşam döngüsü):** Her terminal sonuçtan sonra akışın zamanlayıcısı iptal edilmiş olur ve `StreamTimeouts.pendingTimers()` 0'a döner. Terminal sonuçlar: completed, upstream error, client gone, shutdown, overflow, timeout. Pozitif kontrol: aktif bir akışta değer 1'dir. `StreamTimeouts` bir `SmartLifecycle`'dır. Registry'den sonra durur, `stop()` sınırlıdır (≤ 1 s) ve sonrasında executor sonlanmış olur. Durmuş scheduler'a kurma denemesi istisna fırlatmaz.
- **AC7 (metrik):** `timeouts_total{kind=first_chunk|idle}` counter'ı önceden kayıtlıdır, yani başlangıçta 0 okunur. Yalnızca CAS kazananı artırır. `/actuator/metrics/timeouts_total` 200 döner.
- **AC8 (konfigürasyon):**
  - Varsayılanlar: `lab.timeouts.first-chunk=30s`, `lab.timeouts.idle-between-chunks=30s`.
  - Relaxed binding çalışır.
  - Uygulama şu değerlerle başlamaz: sıfır ya da negatif değerler; `first-chunk ≥ lab.upstream.stream-deadline`; `idle-between-chunks ≥ lab.upstream.stream-deadline`. Hata mesajı property adını içerir.
- **AC9 (regresyon):** Mevcut 49 gateway testi ve 12 simulator testi yeşil kalır. `git diff --stat -- proto simulator` boştur. `gateway/src` altında `Thread.sleep` yoktur. `./mvnw -B verify` yeşildir.
- **AC10 (README):** Timeouts bölümü, ayar ve metrik tablosu satırları ve güncellenmiş "Known limits" eklenir. Hipotezler hipotez olarak kalır.

## 2. İlgili dosyalar, mevcut yaklaşım ve önerilen değişiklik

### Doğrulanmış gerçekler (kod okundu)
Kök paket `gateway/src/main/java/io/github/cemharput/streamlab/gateway/`.

**`stream/StreamBridge`**
- `enum StreamOutcome { COMPLETED, UPSTREAM_ERROR, CLIENT_GONE, SHUTDOWN_CANCELLED }` ve `AtomicReference<StreamOutcome> terminal` vardır.
- CAS yapılan yerler: `writeChunkLocked` (last, `sendLock` altında), `handleUpstreamEnd`, `overflow`, `cancelForShutdown`, `clientGone`. `writeChunkLocked`'taki son yazım hatası claim değildir, `terminal.set(CLIENT_GONE)` yapar.
- Yürütücüler: `TERMINAL_EVENT_EXECUTOR` ve `DRAIN_EXECUTOR` sanal thread'dir. `SEND_LOCK_WAIT=5s`'dir. `sendErrorAndComplete` `tryLock` kullanır.
- `beforeStart` sırası: `requestStream` yayınlanır, `disableAutoRequestWithInitial(queueCapacity)` çağrılır, sonra claim varsa iptal edilir.
- `onNext` sırası: terminal kontrolü, `metrics.chunkQueued()`, `offer`; offer başarısızsa `chunkDequeued()` ve `overflow()`, sonra `scheduleDrain()`.
- `writeChunk` başarılı ve `last` olmayan yazımdan sonra, lock dışında ve `terminal==null` ise `request(1)` çağırır.
- `cancelMessage(outcome)` iki değerlidir.
- `finish` registry'den çıkarır ve INFO log "Stream {0} finished: {1}" yazar.
- Overflow ve upstream-error yollarında `finish` terminal yazımdan sonra çalışır. Shutdown ve client-gone yollarında hemen çalışır.

**`stream/ActiveStreamRegistry`**
- Faz `LifecyclePhases.STREAM_ADMISSION = DEFAULT_PHASE-512`'dir.
- `stop()`: `accepting=false`, `awaitEmpty(drain)`, `cancelAll()` (paralel), `FORCED_CANCEL_WAIT=2s`.
- Registry lock'u hiçbir yazım sırasında tutulmaz. `releasePermit` bu lock'u kısa süre alır.

**`stream/StreamController`**
- Sıra: lease alınır, `SseEmitter(streamDeadline+5s)` oluşturulur, `new StreamBridge(..., streamQueueCapacity, metrics)` kurulur, `registry.admit`, ardından `newStub(...).withDeadlineAfter(streamDeadline).generateTokens(req, bridge)`. `RuntimeException` gelirse `bridge.onError(INTERNAL)`.

**`stream/StreamMetrics`**
- `@Component`, constructor `(MeterRegistry, ActiveStreamRegistry)`.
- `rejected_total` nedenleri `REJECT_REASONS` ile önceden kayıtlıdır.

**`config/GatewayProperties`**
- `record(connectionMode, upstream, shutdown, limits)`, iç içe record'larda compact doğrulama, `requirePositive`, `Limits.defaults()`.
- Üst seviye record'da compact constructor yok.

**`LifecyclePhases`**
- `UPSTREAM_CHANNEL = DEFAULT_PHASE-4096`, `STREAM_ADMISSION = DEFAULT_PHASE-512`.

**`application.yaml`**
- `lab.upstream.stream-deadline: 60s`, `timeout-per-shutdown-phase: 20s`, virtual threads açık.

**Test altyapısı**
- `StreamTestSupport.startedRegistry(...)` repodaki tek `new GatewayProperties(...)` çağrısıdır (grep ile doğrulandı).
- `newBridge(...)` aşırı yüklemeleri `new StreamBridge(...)` kurar. Controller dışında `StreamBridge` kuran tek yer burasıdır.
- `RecordingEmitter`: gate, `sendInProgress`, `completed`. `SteppingEmitter`. `FakeUpstream`: `initialRequest`, `requested`, `delivered`, `credit()`, `cancelMessage`. `CountingLease`.
- `OverloadPolicyTest#simulatorStats()` taze bir in-process kanal açar (`InProcessSimulatorConfig#serverName()`).
- `UpstreamFlowControlTest` Spring olmadan in-process `SimulatorServer` ve gerçek stub kullanır.
- Mevcut zaman aşımı ayarları: `StreamBridgeTest` `@Timeout(10, SEPARATE_THREAD)` ve `QUIET=300ms`, `ActiveStreamRegistryTest` `@Timeout(15, ...)`.

**Simulator**
- `Generation` HANG'de hiçbir şey planlamaz; yalnızca cancel, deadline ya da shutdown ile biter.
- `onCancel` çağrısı `finish(CANCELLED)` yapar; bu `active--` ve `cancelled_total++` demektir.
- Akış kontrolü `observer.isReady()` ile uygulanır.
- `RequestLimits.DEFAULT.maxDelayMs=60000`.
- `SimulatorStats` alanları: `active_generations`, `started_total`, `completed_total`, `cancelled_total`, `faulted_total`, `chunks_emitted_total`, `flow_control_pauses_total`.
- `InProcessSimulatorConfig.simulatorServer()` Spring bean'idir (`destroyMethod="stop"`); context kapanınca simulator da durur.

**Mevcut testlerin HANG kullanımı**
- `GracefulShutdownTest` (drain 3s) ve `OverloadPolicyTest#shutdownReleasesWaitingRequests` (drain 2s) HANG@2 kullanır. 30s'lik varsayılan idle timeout bu testlerde ateşlenmez, yani testler etkilenmez.

### Tasarım kararları

**D1. Zamanlayıcı nerede yaşar: tek paylaşımlı scheduler bean'i.**
- Yeni `stream/StreamTimeouts` sınıfı public'tir, `@Component` ve `SmartLifecycle` uygular.
  - Sahip olduğu executor: `ScheduledThreadPoolExecutor(1)`, daemon platform thread, adı `stream-timeouts`. `setRemoveOnCancelPolicy(true)` ile iptal edilen görev kuyruktan hemen çıkar; böylece bellek tutulmaz ve sızıntı ölçülebilir olur.
  - Ek olarak `setExecuteExistingDelayedTasksAfterShutdownPolicy(false)` ayarlanır.
- API:
  - `public StreamTimeouts(GatewayProperties)`.
  - `Duration firstChunk()`, `Duration idleBetweenChunks()`.
  - package-private `ScheduledFuture<?> schedule(Runnable, Duration)`. Çalışmıyorsa `RejectedExecutionException` fırlatır.
  - `public int pendingTimers()`: `executor.getQueue().size()`, durmuşsa 0.
  - Lifecycle metotları.
- Gerekçe:
  - Akış başına tek bekleyen görev vardır, sayı en fazla `max-active-streams` olur. Görevin kurulması ve iptali O(log n) maliyetlidir.
  - Akış başına sanal thread + sleep çözümü thread başına bekleme ve interrupt tabanlı reset gerektirir, bekleyen sayısı da ölçülemez. Ayrıca ürün kodunda sleep olur.
  - `CompletableFuture.delayedExecutor`, JDK'nın yönetilmeyen statik scheduler'ını kullanır: lifecycle yoktur, bekleyen sayısı yoktur.
  - Simulator da aynı deseni kullanıyor (ADR-0001, paylaşımlı `ScheduledExecutorService`).
- Executor `start()` içinde oluşturulur. `stop()` sırası: `shutdownNow()`, dönen görev sayısı > 0 ise tek WARNING ("sızıntı göstergesi"), sonra `awaitTermination(1s)`.
- Faz: yeni `LifecyclePhases.STREAM_TIMEOUTS = SmartLifecycle.DEFAULT_PHASE - 768`. Faz değeri küçük olan önce başlar ve en son durur:
  - Başlangıçta zamanlayıcılar registry'den (−512) önce başlar, yani kabul edilen hiçbir akış durmuş scheduler görmez.
  - Duruşta registry önce durur. `stop()` döndüğünde her izlenen akışın terminal sonucu claim edilmiştir: ya drenaj sırasında bitmiştir ya da `cancelAll` ile iptal edilmiştir; `accepting=false` sonrası yeni kabul yoktur. Bu yüzden zamanlayıcılara artık gerek kalmaz.
  - Drenaj sırasında zamanlayıcılar çalışmaya devam eder: drenajda asılı kalan bir akış timeout ile erken bitebilir; bu dokümante edilir.
  - Ardından web graceful (Boot sabiti, bkz. varsayımlar) ve kanal (−4096) durur.

**D2. Akış başına durum: yeni `stream/StreamTimer` (package-private, final).**
- Tüm durum yaprak bir lock altındadır: `ReentrantLock timerLock`. Bu lock altında yalnızca `schedule` ve `cancel` çağrılır; yazım, `sendLock` ya da registry lock'u asla alınmaz. Lock sırası `sendLock → timerLock` olabilir (COMPLETED claim'i `sendLock` altında `close()` çağırır); ters yön hiç yoktur.
- Alanlar: `boolean closed, firstChunkSeen, upstreamDone; int outstanding; Armed armed`. `Armed` bir token'dır: `TimeoutKind kind` ve `ScheduledFuture<?> future` taşır.

| Olay (çağıran thread) | Durum değişimi | Zamanlayıcı |
|---|---|---|
| `start(int credit)`: `beforeStart` (istek thread'i, `generateTokens` içinde) | `outstanding=credit` | `!closed` ise FIRST_CHUNK kurulur |
| `chunkReceived(boolean last)`: `onNext` (gRPC thread'i) | `outstanding--`, `firstChunkSeen=true`, `last` ise `upstreamDone=true` | Mevcut iptal edilir; `!closed && !upstreamDone && outstanding>0` ise **yeni** IDLE kurulur (reset) |
| `creditGranted()`: drainer, `request(1)`'den **hemen önce** | `outstanding++` | `!closed && !upstreamDone && firstChunkSeen && armed==null && outstanding>0` ise IDLE kurulur (yalnızca 0→1 geçişinde; zaten kuruluysa reset yok) |
| `upstreamEnded()`: `onError`/`onCompleted` | `upstreamDone=true` | İptal edilir |
| `close()`: her CAS kazananı | `closed=true` (idempotent) | İptal edilir, bir daha kurulmaz |
| Görev ateşlenir (`stream-timeouts` thread'i) | `armed != token` ise eski görevdir, no-op; değilse `armed=null` | Lock **bırakıldıktan sonra** callback `onExpiry(kind)` çağrılır |

- `schedule` `RejectedExecutionException` fırlatırsa DEBUG log yazılır ve `armed=null` kalır. Akış o durumda gRPC deadline'ına ve shutdown iptaline kalır.
- Test erişimcileri: `boolean isArmed()`, `TimeoutKind armedKind()`.
- Kredi sayımı neden doğru: Gerçek gRPC verilen krediden fazla mesaj teslim etmez. `outstanding + queued + inWrite == capacity` değişmezi (önceki plan D3) korunur. `creditGranted` `request(1)`'den önce çalıştığı için `onNext`'teki azaltma her zaman ondan sonra sıralanır. Kurma ve sökme kararı lock altında son değere göre verilir. Bu yüzden aynı anda gelen `chunkReceived` ve `creditGranted` sırasından bağımsız olarak son durum "kurulu ⇔ outstanding>0" olur.

**D3. Zaman aşımı semantiği (soru 2'nin cevabı).**
- İlk chunk zamanlayıcısı `beforeStart`'ta başlar. gRPC stub bunu `call.start`'tan hemen önce çağırır; o anda admission ve WAIT beklemesi geride kalmıştır. Böylece süre yalnızca upstream'in ilk token süresini (TTFT) ölçer. Controller'daki çağrı sırası değişmez. Admission reddedilen bir istekte `beforeStart` hiç çağrılmaz, yani zamanlayıcı da kurulmaz.
- Ölçülen olay chunk'ın **alınmasıdır** (`onNext`), yazılması değil. Yazım istemciye bağlıdır ve timeout'lar upstream'i ölçer.
- Idle zamanlayıcı yalnızca upstream'in göndermeye izni varken kuruludur (`outstanding>0`). Kuyruk dolu ya da drainer takılı olduğu için kredi 0'a indiyse kurulu değildir (AC3b). Drainer takılı ama kredi > 0 iken upstream susarsa bu gerçek upstream sessizliğidir ve timeout ateşlenir. Terminal yazım yine `tryLock(5s)` ile sınırlıdır.
- `last` chunk alındıktan ya da upstream sonu kaydedildikten sonra zamanlayıcı yoktur. COMPLETED claim'i sonradan, yazım anında gelir; arada yavaş bir istemci timeout'a yol açmaz.
- gRPC deadline (`streamDeadline`) toplam üst sınır olarak kalır. Timeout değerleri bundan kesin küçük olmak zorundadır (D6). Idle zamanlayıcı deadline'a yakın kurulursa önce deadline dolabilir. O durumda sonuç `UPSTREAM_ERROR` / `DEADLINE_EXCEEDED` olur, mesaj gRPC'nin mesajıdır.

**D4. Ateşlenme terminal sonuca nasıl dönüşür (StreamBridge).**
- Yeni outcome değerleri: `FIRST_CHUNK_TIMEOUT`, `IDLE_TIMEOUT`. Log satırı bugünkü gibi "Stream {id} finished: IDLE_TIMEOUT" olur.
- Yeni `stream/TimeoutKind` (package-private enum):
  - `FIRST_CHUNK("first_chunk", FIRST_CHUNK_TIMEOUT, "first chunk timeout")`
  - `IDLE("idle", IDLE_TIMEOUT, "idle timeout between chunks")`
  - Alanlar: `tag()`, `outcome()`, `message()`.
- Yeni yardımcı metot `private boolean claim(StreamOutcome o)`: `terminal.compareAndSet(null, o)` başarılıysa `timer.close()` çağırır. **Tüm** CAS noktaları bunu kullanır: `writeChunkLocked` (last), `handleUpstreamEnd`, `overflow`, `cancelForShutdown`, `clientGone` ve yeni `timedOut`. Böylece hiçbir terminal yol zamanlayıcıyı açık bırakamaz.
- `timedOut(TimeoutKind k)` `stream-timeouts` thread'inde çalışır, hiçbir lock tutmaz:
  1. `claim(k.outcome())` kaybedilirse döner.
  2. `cancelUpstream(k.message())`.
  3. `releaseCapacity()` (tam bir kez; yalnızca kazanan).
  4. `metrics.timedOut(k)`.
  5. `discardQueued()`.
  6. `TERMINAL_EVENT_EXECUTOR.execute(() -> { sendErrorAndComplete("DEADLINE_EXCEEDED", k.message()); finish(k.outcome()); })`.

  Bu desen `overflow` ile aynıdır. Akış terminal yazımı dönene kadar tracked kalır. Scheduler thread'i asla `sendLock` beklemez.
- `cancelMessage(outcome)` bir `switch` olur: SHUTDOWN için `SHUTDOWN_MESSAGE`, timeout'larda `k.message()`, diğerlerinde "client disconnected".
- Diğer kancalar:
  - `beforeStart`: bugünkü üç adımdan sonra `timer.start(queueCapacity)` çağrılır. Claim varsa timer kapalıdır, kurulmaz.
  - `onNext`: terminal kontrolünden sonra, `chunkQueued()`'dan önce `timer.chunkReceived(chunk.getLast())` çağrılır. Overflow claim'i timer'ı kapatır.
  - `onError`/`onCompleted`: `upstreamEnd` set edildikten sonra `timer.upstreamEnded()` çağrılır.
  - `writeChunk`: `request(1)`'den hemen önce, aynı `if` içinde `timer.creditGranted()` çağrılır.
- Constructor'a yedinci parametre eklenir: `StreamTimeouts timeouts`. Test için package-private `boolean timerArmed()` eklenir.
- Sabitler: `FIRST_CHUNK_TIMEOUT_MESSAGE` ve `IDLE_TIMEOUT_MESSAGE` testler için `TimeoutKind` üzerinden erişilebilir.

**D5. Etkileşim matrisi (zamanlayıcı ateşlendi, rakip olay ile karşılaşma).** Her satırda yalnızca CAS kazananı `releaseCapacity()` çağırır ve en fazla bir terminal olay yazılır.

| Rakip | Rakip önce | Zamanlayıcı önce (token kontrolünü geçmiş) |
|---|---|---|
| Upstream chunk geldi | `chunkReceived` yeni token kurar; eski görev no-op, akış sürer | Timeout claim'i kazanır; chunk `onNext` başında atılır ya da drainer onu discard eder; `error` DEADLINE_EXCEEDED (sınırda kabul edilen yarış) |
| Upstream error/complete | `upstreamEnded` timer'ı söker; kuyruk boşalınca UPSTREAM_ERROR ya da COMPLETED | Timeout kazanır; upstream iptali bitmiş çağrıda no-op; `handleUpstreamEnd` CAS'ı kaybeder; `error` DEADLINE_EXCEEDED, kuyruk atılır |
| İstemci gitti (emitter callback / yazım hatası) | CLIENT_GONE, olay yok, timer kapalı | Timeout kazanır; `clientGone` kaybeder; error yazımı denenir, IOException DEBUG loglanır |
| Shutdown iptali | SHUTDOWN_CANCELLED, `error` CANCELLED, timer kapalı | Timeout kazanır; `cancelForShutdown` tamamlanmış future döner; akış timeout yazımı dönene kadar tracked kalır ("still finishing" uyarısı mümkün) |
| Son chunk / COMPLETED claim | Son chunk alınınca timer söküldü; COMPLETED `close()` yapar; eski görev CAS'ı kaybeder | Timeout kazanır; `writeChunkLocked` CAS'ı kaybeder, yazmaz; `error` DEADLINE_EXCEEDED |
| Overflow | RESOURCE_EXHAUSTED, timer kapalı | Timeout kazanır; `onNext` terminali görüp döner |
| gRPC deadline | "Upstream error" satırı ile aynı (UPSTREAM_ERROR, gRPC mesajı) | Aynı |

**D6. Konfigürasyon.**
- Yeni nested record:
  ```java
  public record Timeouts(
          @DefaultValue("30s") Duration firstChunk,
          @DefaultValue("30s") Duration idleBetweenChunks) {
      public Timeouts {
          requirePositive(firstChunk, "lab.timeouts.first-chunk");
          requirePositive(idleBetweenChunks, "lab.timeouts.idle-between-chunks");
      }
      public static Timeouts defaults() { return new Timeouts(Duration.ofSeconds(30), Duration.ofSeconds(30)); }
  }
  ```
- `GatewayProperties`'e beşinci bileşen olarak `@DefaultValue Timeouts timeouts` eklenir.
- Üst record'a yeni compact constructor gelir. Null-safe olarak kontrol eder: `firstChunk >= upstream.streamDeadline` ya da `idleBetweenChunks >= upstream.streamDeadline` ise `IllegalArgumentException("lab.timeouts.first-chunk (30s) must be shorter than lab.upstream.stream-deadline (20s)")`. Mesaj iki property adını da içerir.
- **0/off desteklenmez.** Gerekçe:
  - Mevcut tüm süre ayarları pozitif olmak zorundadır.
  - "Kapalı" durumu ayrı bir dal ve test durumu demektir; istek bunu istemiyor.
  - Pratikte devre dışı bırakmak için değer `stream-deadline`'ın hemen altına ayarlanabilir. gRPC deadline zaten üst sınırdır.
- **≥ deadline uyarı yerine reddedilir.** Gerekçe: böyle bir zamanlayıcı hiç ateşlenemez, yani yanlış yapılandırmadır. Mevcut ilke de "geçersiz değer başlangıçta durdurur" şeklindedir. Bedeli: `stream-deadline` 30s'nin altına indirilirse iki timeout da indirilmelidir. Bu README'ye yazılır. Bugün `stream-deadline` değerini değiştiren test yoktur (grep).
- Relaxed binding: `first-chunk`, `firstChunk` ve `LAB_TIMEOUTS_FIRSTCHUNK` biçimleri `@ConfigurationProperties` ile çalışır.
- Etkilenen kurucular:
  - `StreamTestSupport.startedRegistry` beşinci argüman olarak `Timeouts.defaults()` alır.
  - Yeni yardımcı: `properties(Duration drain, Limits, Timeouts)`.

**D7. Metrik.**
- `StreamMetrics` içine `static final List<String> TIMEOUT_KINDS = List.of("first_chunk","idle")` eklenir. Constructor'da `Counter.builder("timeouts_total").tag("kind", k)` ile önceden kayıt yapılır. Yeni metot `void timedOut(TimeoutKind)`.
- Yarış güvenliği: Micrometer counter'ı thread-safe'tir. Artırımı yalnızca CAS kazananı yapar ve terminal yazımdan önce yapar, yani akış başına en fazla bir kez artar.
- Ek bekleyen-zamanlayıcı gauge'u **eklenmez**. Sızıntı kanıtı public test erişimcisi `StreamTimeouts.pendingTimers()` ile yapılır (ActiveStreamRegistry'nin `activeCount()` deseni). Gerekçe: istekte istenmiyor, operasyonel anlamı zayıf, tag kardinalitesi yok. İstenirse sonradan tek satırla gauge yapılabilir.

## 3. Uygulama adımları

1. `config/GatewayProperties.java`: `Timeouts` record'u, beşinci bileşen ve üst seviye compact constructor eklenir (D6).
2. `gateway/src/main/resources/application.yaml`: `lab.timeouts.first-chunk: 30s` ve `lab.timeouts.idle-between-chunks: 30s` eklenir.
3. `LifecyclePhases.java`: `STREAM_TIMEOUTS = SmartLifecycle.DEFAULT_PHASE - 768` eklenir ve javadoc'taki duruş sırası güncellenir: admission, timeouts, web server, channel.
4. Yeni `stream/TimeoutKind.java` (D4).
5. Yeni `stream/StreamTimeouts.java` (D1).
6. Yeni `stream/StreamTimer.java` (D2): state tablosu birebir uygulanır, lock yaprak lock olarak kalır.
7. `stream/StreamMetrics.java`: `timeouts_total` eklenir (D7).
8. `stream/StreamBridge.java`: yeni outcome değerleri, `claim()` yardımcı metodu, `timedOut`, kancalar, `cancelMessage` switch'i, constructor parametresi, `timerArmed()` ve javadoc'a zamanlayıcı paragrafı (D4).
9. `stream/StreamController.java`: `StreamTimeouts` inject edilir ve `StreamBridge`'e geçirilir. Başka değişiklik yoktur.
10. Test desteği:
    - `StreamTestSupport`:
      - Mevcut `newBridge` aşırı yüklemeleri paylaşımlı, lazy başlatılan ve uzun süreli (30s/30s) statik bir `StreamTimeouts` kullanır. Daemon thread'dir; mevcut testlerin davranışı değişmez.
      - Yeni `newBridge(..., StreamTimeouts)` aşırı yüklemesi eklenir.
      - Yeni `startedTimeouts(Duration first, Duration idle)` eklenir; testler bunu `@AfterEach` içinde `stop()` ile durdurur.
      - `properties(...)` yardımcısı eklenir.
    - `testsupport/InProcessSimulatorConfig`: `public SimulatorStats stats()` eklenir; taze kanal açar, işi bitince `shutdownNow` yapar (OverloadPolicyTest'teki kodla aynı). `OverloadPolicyTest` değiştirilmez.
11. Testler (Bölüm 4 tablosu): yeni `stream/StreamTimerTest`, yeni `stream/StreamBridgeTimeoutTest`, yeni `gateway/TimeoutAndCancelTest`; `UpstreamFlowControlTest` ve `ConnectionModeConfigTest` genişletilir.
12. `README.md` güncellenir:
    - Durum satırı ve modül tablosu.
    - "Bounded resources" sonrasına yeni alt bölüm "Timeouts and cancellation". İçeriği: iki zamanlayıcı, nereden ölçüldükleri (çağrı başlangıcı, alınan chunk), kredi 0 iken idle'ın durması, `error`/`DEADLINE_EXCEEDED` ve mesajlar, gRPC deadline ile farkın mesajdan anlaşılması, drenaj sırasında timeout'un sürmesi, iptallerin `active_generations` ile test edildiği.
    - Metrik tablosuna `timeouts_total{kind}` eklenir.
    - Ayar tablosuna iki satır eklenir: "positive, < stream-deadline".
    - "Known limits" güncellemeleri:
      - (a) Zamanlayıcılar takılmış istemci yazımını tespit etmez. Kredi biterse idle durur. Takılı ara yazım maddesi bu yüzden geçerliliğini korur.
      - (b) Emitter `onTimeout` hipotezi aynen hipotez olarak kalır.
      - (c) Zamanlayıcı, tam sınırda gelen bir chunk ile yarışı kazanabilir.
      - (d) Zamanlayıcı tek thread'dir: yapılandırılan süreden erken ateşlenmez, yük altında geç ateşlenebilir.
      - (e) Upstream'i kredi varken susan (ör. HANG) ve istemcisi kopmuş bir akış en geç timeout ile biter. Kopmanın Tomcat'te yazım olmadan algılanıp algılanmadığı doğrulanmamıştır.
      - (f) `stream-deadline` 30s'nin altına indirilirse iki timeout da indirilmelidir.
13. Değiştirilmeyecek dosyalar: `proto/`, `simulator/`, `pom.xml`, `upstream/*`.

## 4. Doğrulama

Genel kurallar:
- Zamanlamalı tüm test sınıfları `@Timeout(value=N, threadMode=SEPARATE_THREAD)` taşır: unit testlerde 15, e2e'de 60.
- Gate'ler ve semaphore'lar `finally` içinde açılır.
- `Thread.sleep` yoktur. "Hiçbir şey olmaz" iddiaları `await().during(X).atMost(Y)` ile, X ≥ 3×timeout olacak şekilde yazılır.
- Unit testlerde timeout'lar 300 ms, `TIMEOUT=5s`.
- `StreamTimeouts` örneği `@AfterEach` içinde durdurulur.

| AC | Test (sınıf#metot) | Beklenen |
|---|---|---|
| AC1 | `StreamBridgeTimeoutTest#firstChunkTimeoutClaimsOnceCancelsUpstreamAndReleasesCapacity` (max=1 registry, `CountingLease`, `FakeUpstream`, chunk yok) | Tek event (error, DEADLINE_EXCEEDED, "first chunk timeout"); `cancelMessage`="first chunk timeout"; `closes==1`; `admittedCount==0`; `activeCount==0`; `timeouts_total{first_chunk}==1`; `during(1s)` boyunca events 1 kalır |
| AC1, AC4 | `TimeoutAndCancelTest#firstChunkTimeoutOnHangCancelsUpstream` (`--lab.timeouts.first-chunk=1s --lab.limits.max-active-streams=1`, `/stream?chunks=3&fault=HANG&faultAtSeq=0`) | `started_total` +1; tek terminal `error` DEADLINE_EXCEEDED, chunk yok; geçen süre ≥1s ve <10s; `await().atMost(10s)` sonunda `active_generations==0` ve `cancelled_total` +1; counter `first_chunk==1`, `idle==0`; `active_streams==0`; `pendingTimers()==0`; sonraki `chunks=3&intervalMs=10` akışı 200 ve `complete` |
| AC1 | `TimeoutAndCancelTest#slowFirstChunkTimesOut` (`firstDelayMs=20000`, first-chunk 1s) | Aynı sonuç: error, `active_generations==0` |
| AC2, AC4 | `TimeoutAndCancelTest#idleTimeoutOnHangMidStreamCancelsUpstream` (idle 1s, `chunks=10&intervalMs=10&fault=HANG&faultAtSeq=3`) | `chunkIds` 0,1,2; ardından tek error "idle timeout between chunks"; `idle==1`; `active_generations==0`; `cancelled_total` +1; `pendingTimers()==0` |
| AC2 | `TimeoutAndCancelTest#slowIntervalTimesOut` (`chunks=5&intervalMs=20000`, idle 1s) | Chunk 0, sonra error DEADLINE_EXCEEDED; `active_generations==0` |
| AC3a | `StreamBridgeTimeoutTest#eachReceivedChunkResetsIdleTimer` (responsive emitter, idle 300ms; 4 kez: chunk ver, `during(200ms)` timeout yok) | Toplam ~800ms'de timeout yok; teslim durunca `await` içinde error idle; counter 1 |
| AC3b | `StreamBridgeTimeoutTest#idleTimerPausedWhileCreditExhausted` (capacity 2, stalled emitter, 2 chunk ile kredi 0) | `during(1s)`: `cancelMessage==null`, counter 0, `timerArmed()==false`, `pendingTimers()==0`; gate açılınca `request(1)` gelir ve timer kurulur (`timerArmed`); yeni chunk verilmezse idle timeout ateşlenir, error olayı chunk'lardan sonra gelir |
| AC3b (gerçek gRPC) | `UpstreamFlowControlTest#stalledClientDoesNotTriggerIdleTimeout` (capacity 4, 50 chunk, interval 1ms, idle 500ms, stalled emitter) | `flow_control_pauses_total>=1` sonrası `during(1500ms)`: `active_generations==1`, `timeouts_total` 0, `activeCount==1`; gate açılınca 50 chunk ve `complete`; `active_generations==0`; `pendingTimers()==0` |
| AC3c | `StreamBridgeTimeoutTest#upstreamEndAndLastChunkDisarmTimer` (iki senaryo, stalled emitter: chunk0 takılı, sonra `onError(UNAVAILABLE)` / `last` chunk) | `during(1s)` timeout yok; gate açılınca: chunk, UNAVAILABLE error / chunk'lar ve `complete` |
| AC3d | `StreamTimerTest#firstChunkArmedOnlyOnStart` birim seviyesinde | `start()` çağrılmadan `isArmed()==false`; çağrılınca FIRST_CHUNK |
| AC4 (negatif kontrol + shutdown) | `TimeoutAndCancelTest#hangWithoutCancelStaysActiveUntilShutdownCancels` (varsayılan timeout'lar, `--lab.shutdown.drain-timeout=1s`, HANG@2) | 2 chunk sonrası `pendingTimers()==1` (pozitif kontrol); `await().during(2s).atMost(5s)` boyunca `active_generations==1`; sonra test thread'i `registry.stop()` çağırır (simulator bean'i hâlâ çalışır); istemci tek error CANCELLED alır; `active_generations==0` ve `cancelled_total` +1 (≤10s); `pendingTimers()==0` |
| AC4 (istemci kopması) | `TimeoutAndCancelTest#clientDisconnectCancelsUpstream` (`chunks=100000&intervalMs=20`) | Önce `active_generations==1`; `disconnect()` sonrası ≤10s içinde `active_generations==0`, `cancelled_total` +1, `active_streams==0`, `pendingTimers()==0` (HANG kullanılmaz; bkz. risk R3) |
| AC5 | `StreamBridgeTimeoutTest#timeoutDoesNotWaitForStalledClientWrite` (capacity 16, stalled, chunk0 takılı, idle 300ms) | Gate kapalıyken `cancelMessage`=idle mesajı, `closes==1`, `admittedCount==0`, `activeCount==1`; gate açılınca events chunk ve error; `activeCount==0`; events 2'de kalır (`during`) |
| AC5 | `StreamBridgeTimeoutTest#timersOfOtherStreamsFireWhileOneTerminalWriteIsStuck` | Birinci akışın error yazımı takılıyken ikinci akış 300ms timeout'unu alır (scheduler bloklanmaz) |
| AC5 | `StreamBridgeTimeoutTest#timeoutThenShutdownCancelGivesOneTerminal` ve `#shutdownCancelThenTimerNeverFires` | İlkinde: `cancelForShutdown()` tamamlanmış future döner, `cancelMessage` timeout mesajıdır, tek error DEADLINE_EXCEEDED. İkincisinde: tek error CANCELLED, `during(1s)` counter 0. Her ikisinde `closes==1` |
| AC5 | `StreamBridgeTimeoutTest#lateUpstreamCallbacksAfterTimeoutSendNothing` | Timeout sonrası `onNext`, `onError`, `onCompleted`: events 1, `closes==1`, `requested==0` |
| AC6 | `StreamBridgeTimeoutTest#noTimerLeftAfterAnyTerminalOutcome` (`@ParameterizedTest`: COMPLETED, UPSTREAM_ERROR, CLIENT_GONE (disconnected emitter), SHUTDOWN, OVERFLOW) | Önce `timerArmed()` true (pozitif); sonra `await` ile `timerArmed()==false` ve `pendingTimers()==0`; `during(1s)` boyunca `timeouts_total` 0 |
| AC6 | `StreamTimerTest` metotları: `firstChunkReplacedByIdleOnFirstChunk`, `exhaustedCreditDisarmsAndGrantedCreditRearms`, `closeIsIdempotentAndPreventsRearm`, `staleExpiryIsIgnored` (eski token callback çağırmaz), `armOnStoppedSchedulerIsIgnored`, `stopTerminatesExecutorWithinOneSecond` | Kurulu tür, `pendingTimers()` değeri ve callback sayısı tabloya uyar; `stop()` <1s; sonrasında `isRunning()==false` ve kurma denemesi istisna fırlatmaz |
| AC6 (lifecycle) | `GracefulShutdownTest` ve `OverloadPolicyTest` (değişmeden) | Yeşil; `close()` süreleri mevcut sınırlarda kalır |
| AC7 | `TimeoutAndCancelTest` tümü ve `StreamBridgeTimeoutTest`: counter değerleri; `TimeoutAndCancelTest#timeoutsCounterIsPreRegisteredAndExposed` | Başlangıçta iki tag 0; `/actuator/metrics/timeouts_total` 200 |
| AC1/2 negatif | `TimeoutAndCancelTest#streamWithinTimeoutsCompletes` (first-chunk 1s, idle 1s, `firstDelayMs=200&intervalMs=100&chunks=10`) | `complete`; counter'lar 0; `completed_total` +1; `cancelled_total` değişmez; `pendingTimers()==0` |
| AC8 | `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` (genişler: 30s/30s), yeni `#timeoutsAreBoundFromRelaxedNames` (`--lab.timeouts.first-chunk=2s`, `--lab.timeouts.idleBetweenChunks=1500ms`), yeni `@ParameterizedTest #invalidTimeoutsFailAtStartup` (`first-chunk=0s`, `-1s`; `idle-between-chunks=0s`; `first-chunk=60s`; `idle-between-chunks=61s`) ve `#timeoutNotShorterThanDeadlineFailsAtStartup` (`--lab.upstream.stream-deadline=10s`) | Değerler eşleşir; her hata yığınında ilgili property adı geçer (deadline vakasında hem `lab.timeouts.first-chunk` hem `lab.upstream.stream-deadline`) |
| AC9 | Coordinator: `./mvnw -B verify`, `git diff --stat -- proto simulator`, `gateway/src` üzerinde `Thread.sleep` araması | Yeşil, boş, boş |
| AC10 | README okunur | Bölüm, tablolar ve hipotez işaretleri yerinde |

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- Tam doğrulama: `./mvnw -B verify`
- Odaklı: `./mvnw -B -pl gateway -am verify -Dtest="StreamTimerTest,StreamBridgeTimeoutTest,StreamBridgeTest,ActiveStreamRegistryTest,UpstreamFlowControlTest,TimeoutAndCancelTest,ConnectionModeConfigTest,GracefulShutdownTest,OverloadPolicyTest" -Dsurefire.failIfNoSpecifiedTests=false`
- Kararlılık: `./mvnw -B -pl gateway -am test` en az iki kez tekrarlanır. Eski build'den kalan `NoClassDefFoundError` görülürse `./mvnw -B -pl gateway clean` çalıştırılır (önceki feature'da yaşandı).

**Riskler:**
- **R1 (yarış ve kilit):** Yeni lock (`timerLock`) yaprak lock kalmalıdır. İçinde yalnızca `schedule` ve `cancel` çağrılır, callback lock dışında çalışır. Reviewer, `claim()` dışında kalan bir CAS noktası olup olmadığını kontrol etmelidir; kalırsa zamanlayıcı sızar.
- **R2 (sınır yarışı):** Timeout ile aynı anda gelen bir chunk kaybedilebilir. Bu kabul edilen bir davranıştır ve README'ye yazılır.
- **R3 (HANG ile kopma algılama):** Tomcat'in yazım olmadan istemci kopmasını algıladığı **doğrulanmamıştır**. Bu yüzden kopma testi sürekli akan bir akış kullanır. HANG + kopma durumunda akışı en geç timeout bitirir.
- **R4 (zamanlama):** e2e testlerinde 1s timeout ve 10s üst sınır kullanılır. `during(2s)` negatif kontrolü toplam test süresini artırır. Sınıf timeout'u 60s'dir.
- **R5 (davranış değişikliği):** Drenaj sırasında HANG akışı varsayılan 30s idle'dan önce shutdown ile iptal edilir; mevcut testler etkilenmez. Üretimde ise drenaj sırasında timeout `CANCELLED` yerine `DEADLINE_EXCEEDED` üretebilir.
- **R6 (konfigürasyon kısıtı):** "Timeout < stream-deadline" kuralı, deadline'ı 30s altına indiren mevcut yapılandırmaları başlatmaz. Bu bilinçli bir karardır ve README'de yazılır.

**Doğrulanmamış varsayımlar (implementer jar'lara ve koda karşı kontrol etmeli):**
- grpc-stub 1.81.0 `ClientCalls`, `beforeStart`'ı `call.start`'tan önce, `generateTokens` çağrısını yapan thread'de çağırır.
- `ScheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true)` ile `cancel(false)` görevi kuyruktan hemen çıkarır, yani `getQueue().size()` bekleyen zamanlayıcı sayısını verir.
- Spring Boot 4.1.1'de web server graceful shutdown fazı `DEFAULT_PHASE-1024`, start/stop fazı `DEFAULT_PHASE-2048`'dir (`WebServerApplicationContext` sabitleri). Böylece −768 değeri registry ile web graceful arasında kalır.
- Spring `DefaultLifecycleProcessor`, `isRunning()==false` olan bean için `stop()` çağırmaz. Test `registry.stop()`'u elle çağırdıktan sonraki `context.close()` için gereklidir; çağırsa bile `stop()` zararsızdır.
- In-process transport'ta kredi 0 iken simulator'ın `isReady()` değeri false olur. `UpstreamFlowControlTest` bunu dolaylı olarak kanıtlıyor. Netty'de mesaj kredisi > 0 iken HTTP/2 penceresinin upstream'i durdurmadığı varsayılır.
- Binding hataları `IllegalArgumentException` mesajını yığına taşır (mevcut `invalidLimitsFailAtStartup` ile tutarlı).
- Emitter `onTimeout` callback'inin bloklu bir yazım sırasında çalışıp çalışmadığı hâlâ hipotezdir ve bu feature onu doğrulamaz.

**Kapsam dışı:** reconnect/retry, per-request bağlantı modu, loadgen, MQTT, bekleyen zamanlayıcılar için Micrometer gauge'u, gerçek TCP seviyesinde takılı istemci testi.

**Açık sorular (engelleyici değil, varsayılanlar kararlaştırıldı):**
- Varsayılan değerler 30s/30s'dir. Değeri ≥ `stream-deadline` olan timeout uyarıyla geçilmez, reddedilir. Kullanıcı farklı varsayılan ya da "uyar" davranışı isterse D6 değişir.
- Hata mesajları sabit metindir ("first chunk timeout" / "idle timeout between chunks") ve süre değerini içermez.
