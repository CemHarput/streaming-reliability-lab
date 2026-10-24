# Plan: gateway-race-tests

Doğrulanmış olgular (HEAD 65631ad, kod doğrudan okundu; MCP code-graph yoktu):

- `StreamBridge` içindeki terminal yolların hepsi `claim()` CAS'ından geçiyor: `writeChunkLocked` (COMPLETED), `handleUpstreamEnd`, `overflow`, `cancelForShutdown`, `timedOut` ve `clientGone`. Claim'i kaybeden taraf hiçbir yan etki üretmiyor: cancel, release ya da yazma yapmıyor.
- `cancelForShutdown`, `clientGone` ve `overflow`, claim'i, cancel'ı ve `releaseCapacity()` çağrısını çağıran thread üzerinde senkron yapıyor. `timedOut` bunları scheduler thread'inde senkron yapıyor. `handleUpstreamEnd` drainer'da `sendLock` tutmadan claim ediyor ve hata olayını aynı thread'de yazıyor.
- Upstream iptal mesajları: CLIENT_GONE için `"client disconnected"`, shutdown için `SHUTDOWN_MESSAGE` (`"gateway shutting down"`), IDLE timeout için `"idle timeout between chunks"`, overflow için `"per-request queue overflow"`. COMPLETED ve `handleUpstreamEnd` upstream'i iptal etmiyor.
- `FakeUpstream.cancelMessage` ilk iptal mesajını tutuyor.
- Overflow yalnızca `queue.offer` başarısız olduğunda tetikleniyor. `RecordingEmitter.stalled()` drainer'ı `sendLock` tutarken chunk 0 yazımında bloklayabiliyor (`StreamBridgeTest#chunkBeyondGrantedCreditFailsWithResourceExhausted` bu deseni kullanıyor).
- `buffered` değeri her `offer`'dan önce artırılıyor ve yalnızca başarılı bir yazmadan sonra azaltılıyor.
- `RetryPolicy.disabled()` için `maxRetries == 0`. Yeniden denenebilen tek kod `UNAVAILABLE`; INTERNAL hiç retry edilmiyor.
- `StreamTimer` IDLE'ı her alınan chunk'ta yeniden kuruyor (`outstanding > 0` ve `callActive` iken). `fire()` token kontrolünü geçtiyse `cancel(false)` onu durduramıyor; bu gerçek bir yarış penceresi.
- `GatewayProperties.Timeouts` yalnızca pozitif süre istiyor. `Shutdown.drainTimeout` da pozitif olmalı.
- `ActiveStreamRegistry.tryRegister` ve `admit` `accepting` bayrağını, `stop()`'un onu `false` yaptığı aynı `lock` altında okuyor. `stop()`'un `cancelAll` snapshot'ı `active` kümesinden alınıyor.
- `StreamController` lease'i admission'dan önce alıyor ve reddedilince kapatıyor (satır 60 ve 74). `RealStream.admit()` `tryRegister`'ı çağırıyor; `RealStream.shutdown()` her akışın kendi `CountingRegistry`'sinde `stop()`'u çağırıyor.
- `StreamContractTest#expectationTableCoversBothFlawsAndOnlyThem` `Scenario.values()` üzerinde dönüyor, ama yeni bir değer eklemek o testi kırmaz. Yine de request "only if the existing shapes are not enough" diyor ve mevcut şekiller yetiyor.
- Kökteki `pom.xml` içinde surefire 3.5.4 ve JaCoCo agent var. Paralel ayar yok. `mvnw` ve `mvnw.cmd` mevcut.

## 1. Davranış ve kabul kriterleri

Yeni bir REAL-only stres sınıfı, `StreamBridge` terminal yollarının altı çiftini ve registry'nin admission-shutdown yarışını iterasyon iterasyon koşturur. Her iterasyonda `StreamContract.check` çalışır (REAL için beklenen küme `{}`). Buna ek olarak testin kendi içinde bir **sonuç tutarlılığı** kontrolü yapılır: görülen terminal olay ile upstream iptal mesajı çifti, o çiftin izin verdiği sonuçlardan biri olmalıdır. Hangi tarafın kazandığı assert edilmez; dağılım loglanır.

- **AC1** `gateway/src/main`, `proto` ve `simulator` altında değişiklik yok: `git diff --stat -- gateway/src/main proto simulator` boş. Mevcut test dosyaları da değişmiyor (`TerminalRaceContractTest`, `StreamContract`, `StreamUnderTest`, `StreamTestSupport`, `LeakyStreamHandler`, `StreamContractTest`); bunlar için `git diff` boş. Tek yeni kaynak dosya `TerminalPathRaceTest.java`. Dokümanda yalnızca `docs/failure-analysis.md` ve `README.md` değişiyor.
- **AC2** `TerminalPathRaceTest` sınıfında 7 test bulunur: `timeoutRacesCompletion`, `timeoutRacesClientGone`, `upstreamErrorRacesClientGone`, `upstreamErrorRacesShutdownCancel`, `overflowRacesClientGone`, `overflowRacesShutdownCancel` ve `admissionRacesStop`. Her birinde, her iterasyon için `StreamContract.check` sonucunun clause kümesi `{}` olur (reddedilen admission'lar hariç, bkz. AC6). Bu ilk altısında lease'in bir kez kapanması, permit'in bir kez bırakılması, `permitsHeld == 0`, `pendingTimers == 0` ve çiftin şekline göre tam bir ya da en fazla bir terminal olay ile sonrasında hiçbir olay olmaması demektir.
- **AC3** Sonuç tutarlılığı: her iterasyonda `(terminal olay türü ve kodu, FakeUpstream.cancelMessage)` çifti bölüm 2'deki tablonun izin verdiği satırlardan biri olur. Bunun anlamı şu: akış kendiliğinden bitmediyse upstream kazananın mesajıyla iptal edilmiştir, kendiliğinden bittiyse (COMPLETED ya da upstream hatası) hiç iptal edilmemiştir. Başka her kombinasyon iterasyon hatasıdır.
- **AC4** Overflow çiftlerinde iki ek deterministik kontrol yapılır. (a) İki taraf döndükten sonra ve gate açılmadan önce `leaseCloses == 1` ve `permitReleases == 1` olur; kapasite, `sendLock` takılı bir yazma tarafından tutulurken serbest kalmıştır. (b) Sonunda tam olarak 1 chunk olayı (chunk 0) görülür; kuyruktaki chunk'lar atılmıştır.
- **AC5** Kayıt yarışında `shutdown()` döndüğü anda her iki sonuçta da `permitsHeld() == 0` olur. Bu değer `stop()` thread'inde, `stop()` dönünce okunur.
- **AC6** Kayıt yarışında her iterasyon iki sonuçtan biridir. **REFUSED**: `admit() == false`; `starter.calls` boş; olay yok; `leaseCloses == 0`, `permitReleases == 0`, `permitsHeld == 0`, `pendingTimers == 0`. **ADMITTED**: contract `EXACTLY_ONE_ANY` sağlanır; sonuç ya `complete` ve `cancelMessage == null`, ya da `error` `CANCELLED` ve `SHUTDOWN_MESSAGE`.
- **AC7** Dağılım her test için tek bir INFO satırıyla loglanır, örneğin `COMPLETE=.., ERROR:DEADLINE_EXCEEDED=..`; assert edilmez. Üç yerel tam build'de her çiftin iki sonucu da görülmüş olmalı (kayıt yarışında REFUSED ve ADMITTED). Görülen dağılımlar implementation.md'ye yazılır. Tek taraflı kalan bir çift bir test hatası değildir: önce sweep ayarlanır, yine tek taraflıysa sınırlama olarak raporlanır.
- **AC8** Hijyen kuralları:
  - `Thread.sleep` yok.
  - Sınıfta `@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)` var.
  - Bütün bariyer, future, latch ve Awaitility beklemeleri sınırlı ve beklenen sürenin en az 5 katı payla kurulu.
  - Gate'ler iterasyonun `finally` bloğunda ve `@AfterEach` içinde açılıyor. Executor `finally` içinde `shutdownNow` ile kapatılıp `awaitTermination` ile bekleniyor. `StreamTimeouts` `@AfterEach` içinde durduruluyor.
  - Harness hataları `"HARNESS: ..."` önekiyle raporlanıyor; ürün ihlalleri ise iterasyon raporuyla (`iteration i [PAIR]: ...`).
- **AC9** Eklenen test süresi yaklaşık 30 s'yi geçmez. Süre surefire raporundaki `TerminalPathRaceTest` değerinden ölçülüp implementation.md'ye yazılır.
- **AC10** `./mvnw -B verify` art arda 3 kez yeşil geçer ve flaky sonuç olmaz.
- **AC11** `docs/failure-analysis.md` yeni bir bölüm alır: stresin ne gösterdiği ve ne göstermediği, hangi yolun `sendLock` tuttuğu, olgular ve hipotezler ayrı. "Running it" komutu güncellenir. README'deki "Stream contract tests" paragrafına 1-2 cümle eklenir. Hiçbir cümle kodla çelişmez.
- **AC12** Bir test gerçek bir defekt bulursa uygulama durur. Bulgu kanıtlarıyla raporlanır ve feature `blocked` olur. `gateway/src/main` değiştirilmez, assert zayıflatılmaz (ayrıntı bölüm 4'te).

## 2. Dosyalar, semboller ve tasarım

### Çiftlerin doğrulanması ve dışarıdan tetiklenmesi

Hiçbir çift düşürülmedi: hepsinde iki bağımsız thread üzerinde gerçek bir `claim()` CAS yarışı var. İki çift yeniden şekillendirildi: timer çiftleri ve kayıt yarışı.

| Çift | A tarafı (tetik) | B tarafı | Pencere (koddan) | Şekil taşıyıcısı |
|---|---|---|---|---|
| `TIMEOUT_VS_COMPLETION` | IDLE timer (scheduler thread, `fire` → `timedOut`) | `deliver(chunk(1,true))` + `onCompleted()`, `base + offset` anında | `fire` token kontrolünü geçtikten sonra `chunkReceived(last)` bir şeyi disarm edemiyor; `timedOut` ile drainer'daki COMPLETED claim'i CAS'ta yarışıyor | `Scenario.RACE_COMPLETION_VS_SHUTDOWN` (`EXACTLY_ONE_ANY`) |
| `TIMEOUT_VS_CLIENT_GONE` | aynı timer | `unit.clientGone()`, `base + offset` anında | `timedOut` ile `clientGone` CAS'ı | `RACE_COMPLETION_VS_CLIENT_GONE` (`AT_MOST_ONE`) |
| `UPSTREAM_ERROR_VS_CLIENT_GONE` | `call.observer().onError(Status.INTERNAL.withDescription("race").asRuntimeException())`: `attemptEnded` → `recordEnd` → `scheduleDrain` → yeni drainer → `handleUpstreamEnd` claim | `unit.clientGone()` | drainer'a geçiş (virtual thread) ile CAS. INTERNAL retry edilmez ve `RetryPolicy.disabled()` kullanılıyor | `AT_MOST_ONE` |
| `UPSTREAM_ERROR_VS_SHUTDOWN` | aynı | `unit.cancelForShutdown()` | aynı | `EXACTLY_ONE_ANY` |
| `OVERFLOW_VS_CLIENT_GONE` | dolu kuyruğa `deliver(chunk(CAPACITY+1,false))`: `offer` başarısız → `overflow()` claim, inline | `unit.clientGone()` | iki inline CAS. Drainer bu sırada chunk 0 yazımında `sendLock`'u tutarak bloklu | `AT_MOST_ONE` |
| `OVERFLOW_VS_SHUTDOWN` | aynı | `unit.cancelForShutdown()` | aynı | `EXACTLY_ONE_ANY` |
| `ADMIT_VS_STOP` | `unit.admit()` (`tryRegister`), true dönerse `unit.start()`; çift iterasyonlarda ek olarak `deliver(chunk(0,true))` + `onCompleted()` | `unit.shutdown()` (`registry.stop()`) | registry `lock`'u: `tryRegister` ile `accepting = false` sıralanıyor | ADMITTED için `EXACTLY_ONE_ANY`; REFUSED ayrı kontrol |

**Timer çiftleri** yeniden şekillendirildi. `timedOut` private olduğu için tek tetik zamanlayıcının kendisi. Ayrı bir `StreamTimeouts` kullanılıyor: `startedTimeouts(Duration.ofSeconds(30), IDLE = Duration.ofMillis(10))`. Akış şöyle:

1. `base = System.nanoTime()` alınır, sonra `deliver(chunk(0,false))` çağrılır; chunk alınınca IDLE 10 ms ile kurulur.
2. B tarafı, bir executor thread'inde `System.nanoTime() >= base + offset_i` olana kadar `Thread.onSpinWait()` ile meşgul bekler. Bu sleep değildir ve en fazla 40 ms sürer.
3. `offset_i = (i % TIMER_STEPS) * (4 * IDLE / TIMER_STEPS)`, `TIMER_STEPS = 20`, yani 0'dan 38 ms'ye 2 ms adımlarla.

Bariyer gerekmiyor çünkü karşı taraf scheduler. Pencere 4×IDLE genişliğinde; böylece Windows timer çözünürlüğü yaklaşık 15,6 ms olsa da iki sonuç da kapsanır (A1). Timer'ın gerçekten kurulduğu bir harness ön koşulu ile kontrol edilir: `deliver` döndükten sonra `await().atMost(5 s).until(() -> unit.pendingTimers() == 1 || unit.leaseCloses() >= 1)`. Yani timer ya kuruludur ya zaten ateşlenmiştir. Bu kontrol, timer'ın hiç kurulmadığı boş bir testi engeller.

**Kayıt yarışı** da yeniden şekillendirildi. Koda göre `stop()` başladıktan sonra (`accepting = false` yapıldıktan sonra) hiçbir admission mümkün değil. Request'teki "drain sırasında admit edilen permit" durumu bu yüzden oluşamaz. Asıl assert edilen ikilik şu:

- Admission `stop()`'tan **önce** olduysa akış `active` snapshot'ındadır. Drain sırasında ya kendisi biter (completing varyantı) ya da `cancelForShutdown` ile iptal edilir. Sonuçta permit bir kez bırakılmış, tam bir terminal olay yazılmıştır.
- Admission **sonra** denendiyse temiz bir red vardır.

Ayrıca `stop()` döndüğünde `permitsHeld == 0` olur (AC5). Bu deterministik: `cancelForShutdown` kapasiteyi `stop()` dönmeden senkron bırakıyor; drain ile biten akış da `finish`'ten önce `releaseCapacity` çağırıyor. Bu yarış için `Wiring.drainTimeout = ADMIT_DRAIN_TIMEOUT = 20 ms` kullanılıyor; böylece kabul edilen boşta bir akış yaklaşık 20-35 ms'de zorla iptal edilir. Admission henüz `start()`'tan önceyken iptal edilirse: `attemptBeforeStart` `terminal != null` görüp `stream.cancel(SHUTDOWN_MESSAGE)` yapar, `timer.start` da timer kapalı olduğu için hiçbir şey kurmaz. Doğrulanan bu davranış yüzünden iptal mesajı her iki sırada da `SHUTDOWN_MESSAGE`.

Kayıt yarışının sınırları:

- Yalnızca `tryRegister` yarıştırılıyor. Üretimdeki `admit(bridge)` aynı lock altında aynı kontrolü yapıyor (satır 80-87), ama ayrıca yarıştırılmıyor.
- WAIT modundaki bekleyenleri mevcut `ActiveStreamRegistryTest#stopReleasesWaitersWithoutDelayingShutdown` kapsıyor.
- Reddedilen akışın lease'ini controller kapatıyor; bu yarışın kapsamında değil, bu yüzden REFUSED'da `leaseCloses == 0` bekleniyor.

**Overflow çiftlerinin hazırlığı** (`OVERFLOW_CAPACITY = 4`, `RecordingEmitter.stalled()`). Önce `deliver(chunk(0,false))` çağrılır ve `await(5 s).until(emitter.sendInProgress)` ile drainer'ın bloklandığı beklenir. Ardından chunk 1..4 teslim edilir. Harness ön koşulu `unit.bufferedChunks() == OVERFLOW_CAPACITY + 1` olur: bloklu chunk 0 artı dolu kuyruk. Yarışan teslim `chunk(5,false)`.

İki taraf `get` ile beklendikten sonra AC4(a) kontrol edilir, sonra `emitter.gate.countDown()` çağrılır (ayrıca `finally` içinde de). Terminal olayın `sendLock`'u `tryLock(SEND_LOCK_WAIT = 5 s)` ile beklediğini unutmamak gerekiyor: gate gecikirse olay atlanır. Gate yarıştan hemen sonra, milisaniyeler içinde açılıyor; risk R4'te.

**Upstream hatası çiftlerinin hazırlığı.** `responsive()` emitter kullanılır. `deliver(chunk(0,false))`'tan sonra harness ön koşulu olarak chunk 0 olayının yazıldığı beklenir (`events.size() == 1`). Böylece drainer boşta olur ve A tarafının `scheduleDrain` çağrısı yeni bir drainer başlatır.

### Sonuç tutarlılık tablosu (AC3)

| Çift | İzin verilen sonuçlar: (terminal olay, `cancelMessage`) |
|---|---|
| TIMEOUT_VS_COMPLETION | (`complete`, null) · (`error code=DEADLINE_EXCEEDED`, `TimeoutKind.IDLE.message()`) |
| TIMEOUT_VS_CLIENT_GONE | (`error DEADLINE_EXCEEDED`, IDLE mesajı) · (yok, `"client disconnected"`) |
| UPSTREAM_ERROR_VS_CLIENT_GONE | (`error INTERNAL`, null) · (yok, `"client disconnected"`) |
| UPSTREAM_ERROR_VS_SHUTDOWN | (`error INTERNAL`, null) · (`error CANCELLED`, `StreamBridge.SHUTDOWN_MESSAGE`) |
| OVERFLOW_VS_CLIENT_GONE | (`error RESOURCE_EXHAUSTED`, `"per-request queue overflow"`) · (yok, `"client disconnected"`) |
| OVERFLOW_VS_SHUTDOWN | (`error RESOURCE_EXHAUSTED`, overflow mesajı) · (`error CANCELLED`, `SHUTDOWN_MESSAGE`) |
| ADMIT_VS_STOP | REFUSED (AC6) · (`complete`, null) · (`error CANCELLED`, `SHUTDOWN_MESSAGE`) |

Kod, `StreamContract` ile aynı yöntemle olay metninden çıkarılır: `isTerminal` ve `contains("code=" + X)`; `ErrorEvent` record'unun `toString`'i bu formu veriyor, mevcut REAL senaryoları da buna dayanıyor. "Yok" satırları yalnızca emitter yazmaları başarılı olduğu için kazananı belirleyebiliyor: responsive emitter kullanılıyor ya da gate açılıyor. Kaybedenin hiçbir şey iptal etmediği kodda doğrulandı.

### Karar 1: Yeni sınıf, mevcut testlere dokunulmuyor

Yeni dosya: `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/TerminalPathRaceTest.java`. Sınıf REAL-only, LEAKY yok. `TerminalRaceContractTest.run`'ın iskeleti burada `Pair` parametreli genel bir `run(Pair pair, int iterations)` olarak yeniden yazılıyor. İskelette korunanlar: iterasyon başına yeni `RecordingEmitter`, `CountingLease`, `FakeUpstreamStarter` ve `StreamUnderTest.create(Kind.REAL, Wiring)`; `CyclicBarrier(2)` ile `await(5 s)`; spin skew; iterasyon başına settle; sonda tek `during(QUIET)` stabilite beklemesi ve iterasyon başına `check`; `winners` logu.

Gerekçe: mevcut sınıf LEAKY ile REAL'i karşılaştırıyor ve AC4/AC5 ile doğrulanmış durumda; ona dokunmak regresyon riski getiriyor. Bedeli yaklaşık 50 satır kopyalanmış settle ve snapshot kodu. Alternatif, ortak bir `RaceHarness` yardımcısı çıkarıp iki sınıfın onu kullanması; bu mevcut testi değiştirir, bu feature'da önerilmiyor.

`Pair` test-içi bir enum. Her değer şunları taşır: şekil taşıyıcı `Scenario`, capacity, emitter türü, iterasyon sayısı ve izin verilen sonuçlar. Hazırlık ve taraflar `switch` ile yazılır.

### Karar 2: Yeni Scenario değeri yok

Gereken şekillerin hepsi mevcut: `EXACTLY_ONE_ANY` ve `AT_MOST_ONE`. Bu yüzden request'in kuralı gereği iki mevcut yarış senaryosu yalnızca şekil taşıyıcısı olarak kullanılıyor. Her iki senaryoda `UpstreamEnd.NOT_MEASURED` ve `queueMeasured = false`, dolayısıyla `check` yalnızca SINGLE_TERMINAL, RELEASE_ONCE ve NO_TIMER_LEAK maddelerine bakar. Rapor metnindeki yanıltıcı senaryo adını dengelemek için uyumsuzluk mesajı `Pair` adıyla başlar: `iteration 17 [OVERFLOW_VS_SHUTDOWN, shape EXACTLY_ONE_ANY]: ...`. Upstream iptali sözleşmenin UPSTREAM_CANCELLED maddesiyle değil, AC3'teki test-içi kontrolle ölçülüyor. O madde simülatör sayaçlarına bağlı ve sözleşme maddeleri değişmiyor.

### Karar 3: Sweep, iterasyon sayısı ve süre bütçesi

Bariyerli çiftlerde iki yönlü sweep kullanılıyor. `k = (i % (2*SPIN_STEPS + 1)) - SPIN_STEPS`, `SPIN_STEPS = 25`, `SPIN_UNIT = 1500` (mevcut sabitlerle aynı). k < 0 ise A, k > 0 ise B `|k| * SPIN_UNIT` kez `onSpinWait` yapar. Mevcut test yalnızca iptal tarafını kaydırıyor ve dağılım `184/16` gibi çarpık çıkıyor. İki yönlü sweep, inline CAS'ı hızlı olan tarafların (clientGone, kayıt) iki sonucunu da görmeyi kolaylaştırıyor.

| Çift | N | Tahmini süre |
|---|---|---|
| TIMEOUT x2 | 100 | her biri 2-4 s (ortalama yaklaşık 20 ms bekleme + settle) |
| UPSTREAM_ERROR x2 | 200 | her biri 1-2 s |
| OVERFLOW x2 | 200 | her biri 1-2 s |
| ADMIT_VS_STOP | 200 | 3-4 s (yaklaşık yarısı kabul edilip 20-35 ms'de iptal) |
| 7 × QUIET 500 ms | | 3,5 s |

Toplam yaklaşık 15-22 s; bütçe 30 s. Süre aşılırsa önce timer N değeri 60'a, kayıt yarışında N 100'e düşürülür ve bu raporlanır.

Bekleme sınırları ve payları:

- Bariyer 5 s, future `get` 10 s, iterasyon settle `atMost(5 s)`: beklenen değerler milisaniye mertebesinde, pay 100 katın üstünde.
- Stabilite `during(500 ms).atMost(5 s)`.
- `@Timeout` 60 s; beklenen süre test başına 5 s'den az, pay 12 kat.

### Karar 4: Harness hatası ile ürün hatasını ayırmak

`"HARNESS: ..."` olarak raporlananlar:

- `admit()` false döndü (kayıt yarışı dışında)
- `starter.calls.size() != 1`
- ön koşullar: `sendInProgress`, `bufferedChunks == CAPACITY + 1`, timer kurulu ya da ateşlenmiş, chunk 0 yazılmış
- `BrokenBarrierException` ya da bariyer veya future `TimeoutException`'ı
- executor `awaitTermination`'ın false dönmesi

Bunlar için `join(Future<?>, String side)` yardımcısı `TimeoutException`'ı ve nedeni `BrokenBarrierException`/`TimeoutException` olan `ExecutionException`'ı `AssertionError("HARNESS: ...")` olarak fırlatır.

Bir taraf ürün kodundan exception alırsa bu `AssertionError("[PAIR] side X threw", cause)` olarak bildirilir; harness hatası sayılmaz.

İterasyon settle beklemesi zaman aşımında **exception fırlatmaz**: false döner, `not settled in 5 s` olarak loglanır ve iterasyon devam eder. Böylece sızıntı gibi gerçek bir ürün kusuru, `check` sonucunda RELEASE_ONCE veya NO_TIMER_LEAK ihlali olarak görünür, belirsiz bir timeout olarak değil.

Settle koşulu: `leaseCloses >= 1 && permitReleases >= 1 && pendingTimers == 0`. Her sonucu terminal olay yazan çiftlerde (`EXACTLY_ONE_ANY` ve ADMITTED) buna terminal olayın görülmesi de eklenir. REFUSED için settle yapılmaz.

## 3. Uygulama adımları

1. `TerminalPathRaceTest.java` oluşturulur (paket `io.github.cemharput.streamlab.gateway.stream`, package-private erişim için zorunlu):
   - Sınıf `@Timeout(value = 60, threadMode = SEPARATE_THREAD)` taşır.
   - Alanlar: `List<StreamTimeouts> timeoutsStarted`, `List<RecordingEmitter> emitters`.
   - `@AfterEach` tüm gate'leri açar ve tüm `StreamTimeouts`'ları durdurur.
2. Sabitler: `CAPACITY = 16`, `OVERFLOW_CAPACITY = 4`, `IDLE = 10 ms`, `TIMER_STEPS = 20`, `SPIN_STEPS = 25`, `SPIN_UNIT = 1500`, `ADMIT_DRAIN_TIMEOUT = 20 ms`, `QUIET = 500 ms`. Iterasyon sayıları Karar 3'teki gibi.
3. `enum Pair` ve `run(Pair, int)` yazılır. Bir iterasyonun sırası şöyle:
   1. yeni bileşenler kurulur;
   2. admit edilir (kayıt yarışı dışında);
   3. `start()` çağrılır;
   4. bölüm 2'deki hazırlık ve ön koşullar;
   5. `barrier.reset()`;
   6. A ve B `newVirtualThreadPerTaskExecutor` üzerinde başlatılır (timer çiftlerinde yalnızca B);
   7. `join`;
   8. AC4(a) ya da AC5 kontrolü;
   9. gate açılır;
   10. settle;
   11. birim, emitter, upstream ve REFUSED bayrağı listelere eklenir.

   Executor ve gate `finally` içinde kapatılır. Ardından `during(QUIET)` stabilite beklemesi gelir. Snapshot'a `Observation` (mevcut 14 alanlı record, ölçülmeyen alanlar -1/null) ile `cancelMessage` girer.
4. Değerlendirme: her iterasyon için `StreamContract.check(pair.scenario, o, capacity)`, AC3 tutarlılığı, overflow için AC4(b) ve REFUSED için AC6 kontrolü. Uyumsuzluklar listede toplanıp tek bir `assertThat(mismatches).isEmpty()` ile kontrol edilir. Ardından `observations` boyutu = N ve `timeouts.pendingTimers() == 0` assert edilir. Dağılım INFO olarak loglanır.
5. Altı `@Test` metodu her biri tek satırda `run(Pair.X, N)` çağırır. Kayıt yarışı (yedinci) için REFUSED ve ADMITTED dalları kontrol edilir.
6. `docs/failure-analysis.md`:
   - "Scenarios" paragrafına `TerminalPathRaceTest`'i işaret eden bir cümle eklenir.
   - "What the REAL stress does and does not show" bölümünden sonra yeni bir bölüm gelir: `## What the terminal-path stress adds`. İçeriği: çift tablosu ve tetikleri; hangi yolların claim ve release'i `sendLock` tutmadan yaptığı (`handleUpstreamEnd`, `timedOut`, `overflow`, `cancelForShutdown`, emitter callback'inden gelen `clientGone`) ve overflow çiftlerinde drainer'ın yarış boyunca `sendLock`'u tutup bloklu kaldığı; kayıt yarışında `stop()`'tan sonra admission olamayacağı ve neyin assert edildiği.
   - Aynı bölümde "göstermediği" kısmı: kanıt değildir (ADR-0002); timer yarışı scheduler çözünürlüğüne bağlıdır; dağılımlar makineye bağlıdır ve tek çekirdekte tek tarafa çökebilir; `admit(bridge)` ve WAIT yolu yarıştırılmamıştır; retry ve cancel yarışı kapsam dışıdır; gerçek container thread'leri kullanılmamıştır. Her biri olgu ya da hipotez olarak etiketlenir.
   - Yerel dağılımlar tarihleriyle yazılır. "Running it" komutuna `TerminalPathRaceTest` eklenir.
7. `README.md` "Stream contract tests" paragrafına 1-2 cümle eklenir: diğer terminal yol çiftleri ve admission-shutdown yarışı da stres altında, ayrıntı `docs/failure-analysis.md`'de.
8. Bölüm 4'teki kontroller çalıştırılır; süreler, test sayıları ve dağılımlar implementation.md'ye yazılır.

## 4. Doğrulama

| AC | Kontrol | Beklenen sonuç |
|---|---|---|
| AC1 | `git diff --stat -- gateway/src/main proto simulator`; mevcut 6 test dosyası için `git diff --stat`; `git status` | boş; tek yeni kaynak dosya `TerminalPathRaceTest.java` |
| AC2-AC6 | `TerminalPathRaceTest` içindeki 7 test | yeşil, her iterasyonda uyumsuzluk yok |
| AC7 | surefire çıktısındaki `race [PAIR]` INFO satırları, 3 build | her çiftte iki sonuç da > 0 (assert edilmez, raporlanır) |
| AC8 | `gateway/src` altında `Thread.sleep` grep'i; dosyanın okunması | eşleşme yok; `@Timeout`, `finally` ve sınırlı beklemeler mevcut |
| AC9 | `gateway/target/surefire-reports/TEST-io.github.cemharput.streamlab.gateway.stream.TerminalPathRaceTest.xml` içindeki `time` | 30 s'den az |
| AC10 | `./mvnw -B verify` ×3 | 3 kez exit 0; gateway test sayısı mevcut + 7 (gerçek sayı raporlanır) |
| AC11 | reviewer'ın doküman-kod karşılaştırması | çelişki yok |

Komutlar (Windows'ta bash ile `./mvnw`, PowerShell ile `.\mvnw.cmd`):

- `./mvnw -B verify` (zorunlu, 3 kez)
- Odaklı: `./mvnw -B -pl gateway -am verify -Dtest=TerminalPathRaceTest,TerminalRaceContractTest -Dsurefire.failIfNoSpecifiedTests=false` (bayrak `docs/failure-analysis.md`'de zaten kullanılıyor)
- Hızlı tekrar: `./mvnw -B -pl gateway -am test -Dtest=TerminalPathRaceTest -Dsurefire.failIfNoSpecifiedTests=false`, 3-5 kez

**Bir test gerçek bir defekt bulursa:** uygulama durur. `gateway/src/main` değiştirilmez. Assert gevşetilmez, retry ya da tolerans eklenmez, iterasyon sayısı sorunu gizlemek için düşürülmez. Önce harness hatası olmadığı doğrulanır: ön koşullar ve `HARNESS` mesajları kontrol edilir. Sıklığı belirlemek için odaklı komut tekrarlanabilir. Bulgu implementation.md'ye kanıtlarıyla yazılır: çift, iterasyon, `StreamContract.names(events)`, sayaçlar, `cancelMessage`, ilgili log satırları ve kod yolu hipotezi (hipotez olarak etiketli). Feature `blocked` olur; düzeltme ayrı bir feature'dır.

Riskler:

- **R1 (flaky, timer):** Windows'ta `ScheduledThreadPoolExecutor` 15,6 ms çözünürlükle geç ateşleyebilir; dağılım kayar. Doğruluk etkilenmez çünkü kazanan assert edilmiyor. 0-40 ms penceresi bunu kapsıyor; gerekirse `TIMER_STEPS` ve pencere ayarlanır.
- **R2 (boş test):** Tek çekirdek ya da aşırı yüklü bir makinede bir çift tek tarafa çökebilir ve stres hiçbir test kırılmadan zayıflar. Bu dağılım logu ve dokümanla görünür kılınıyor; assert edilmiyor (request gereği).
- **R3 (harness hatası ürün hatası gibi görünebilir):** Overflow hazırlığında drainer'ın chunk 0'ı poll etmesi beklenmeden chunk teslim edilirse kuyruk erken dolar. `sendInProgress` ve `bufferedChunks` ön koşulları bunu harness hatası olarak yakalar.
- **R4:** Overflow çiftlerinde gate `SEND_LOCK_WAIT` (5 s) içinde açılmazsa terminal olay atlanır ve EXACTLY_ONE_ANY ihlali gibi görünür. Gate yarıştan hemen sonra açılıyor; bir `join` 5 s'yi aşarsa bu zaten harness hatasıdır.
- **R5:** Virtual thread'lerde busy-spin carrier'ı işgal eder. Paralellik çekirdek sayısı kadar; tek carrier'da taraflar sıralanır ve dağılım çöker (R2'ye bakınız).
- **R6:** Settle beklemesindeki 1 ms poll CPU harcar ve timer yarışını bozabilir. Settle, taraflar döndükten sonra başlıyor, bu yüzden yarışı etkilemez.
- **R7:** Doküman ile kod arasında sapma riski. Her iddia metod adına bağlanmalı; `sendLock` cümleleri bölüm 2'deki doğrulanmış olgularla sınırlı tutulmalı.

Doğrulanması gereken varsayımlar:

- **A1:** IDLE = 10 ms, Windows'ta en geç yaklaşık 16-31 ms içinde ateşleniyor (dağılım logundan görülecek).
- **A2:** `newVirtualThreadPerTaskExecutor` iki tarafı gerçekten eşzamanlı koşturuyor (çekirdek sayısı en az 2).
- **A3:** `SseEmitter.event().data(ErrorEvent)` metni `code=<CODE>` içeriyor. `StreamContract`'ın REAL senaryoları (`code=CANCELLED`, `code=INTERNAL`) buna dayanarak geçiyor; yine de implementer ilk çalıştırmada görmeli.
- **A4:** `RecordingEmitter.simulateClientGone()` bridge'in constructor'da kaydettiği `onCompletion` callback'ini çağırıyor. Kodda doğrulandı: override son callback'i saklıyor ve bridge yalnızca bir tane kaydediyor.
- **A5:** Süre tahminleri JaCoCo agent'ı açıkken ölçülecek.

Açık soru yok. Karar 1 (yeni sınıf) ve Karar 2 (yeni `Scenario` yok, mevcutlar şekil taşıyıcısı) request'in kurallarından çıkarıldı. Kullanıcı mevcut sınıfın genişletilmesini ya da adlandırılmış yeni `Scenario` değerlerini tercih ederse plan küçük bir değişiklikle uyarlanabilir.

İlgili dosyalar:

- `gateway/src/test/java/io/github/cemharput/streamlab/gateway/stream/TerminalPathRaceTest.java` (yeni)
- `TerminalRaceContractTest.java` (örnek iskelet, değişmiyor); `StreamContract.java`, `StreamUnderTest.java`, `StreamTestSupport.java` (kullanılıyor, değişmiyor)
- `StreamBridge.java`, `ActiveStreamRegistry.java`, `StreamTimer.java` (salt okunur)
- `docs/failure-analysis.md`, `README.md` (değişecek)
