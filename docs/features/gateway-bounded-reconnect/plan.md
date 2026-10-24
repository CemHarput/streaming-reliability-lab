# Plan: gateway-bounded-reconnect

Kapsam: upstream `UNAVAILABLE` ile biterse sınırlı sayıda yeniden deneme yapılır. Bekleme üstel artar ve tam jitter'lıdır. Denemelerin toplam süresi `lab.upstream.stream-deadline` bütçesini aşmaz. Kısmi akış, aynı `request_id` ve `seed` ile devam ettirilir; istemci tekrar ya da eksik chunk görmez. Mevcut CAS/claim, kuyruk, kredi ve zamanlayıcı garantileri korunur. Kapsam dışı: per-request bağlantı modu, loadgen, MQTT, gRPC service-config retry'ı, istemciye retry ipucu. `proto/` ve `simulator/` değişmez.

## 1. Davranış ve kabul kriterleri

- **AC1 (kurtarma, baştan başlama):** Hiç chunk alınmamışken `UNAVAILABLE` gelirse en fazla `lab.retry.max-attempts` kez yeniden denenir. Yeni çağrıda `resume_from_seq` set edilmez (0); `request_id`, `seed`, `profile` ve `fault` aynıdır. Kanal hazır olmadığı için hemen başarısız olan çağrı da (interceptor ile taklit edilir) aynı yoldan denenir. Bir deneme başarılı olursa istemci `0..n-1` chunk'larını ve tek bir `complete` olayı alır.
- **AC2 (kısmi akış, resume):** En az bir chunk **alınmışsa** yeni çağrı `resume_from_seq = son alınan seq + 1` ile başlar. Kuyrukta bekleyen (alınmış, henüz yazılmamış) chunk'lar atılmaz, sırayla yazılır. Yeni çağrının chunk'ları onların arkasından gelir. Sonuç: istemci `0..n-1`'i sırayla ve tekrarsız alır; metinler kesintisiz bir çağrıyla byte düzeyinde aynıdır; tek bir `complete` vardır. Yeni çağrının ilk kredisi `capacity − buffered` olur (bkz. D3). Gerçek gRPC'de taşma (`RESOURCE_EXHAUSTED`) olmaz; bu, kredisi 0 olan bir yeni çağrıda da geçerlidir.
- **AC3 (backoff):** Gecikme `random[0, min(max-backoff, initial-backoff·2^k)]` aralığından seçilir; k=0 ilk retry'dır. Rastgelelik ve saat enjekte edilebilir. Tavan dizisi deterministiktir (100, 200, 400, 800, 1600, 2000, 2000 ms) ve taşma olmaz.
- **AC4 (bütçe):** Bütçe, ilk çağrı başlarken (admission'dan sonra) `stream-deadline` olarak başlar. Her yeni çağrının gRPC deadline'ı **kalan** bütçedir. Sıradaki backoff kalan bütçeden küçük değilse, ya da çağrı başlatılacağı anda kalan bütçe ≤ 0 ise retry yapılmaz. İstemciye son upstream durumuyla tek `error` gider.
- **AC5 (yeniden denenmeyenler):** `INTERNAL`, `INVALID_ARGUMENT`, `DEADLINE_EXCEEDED` (gRPC deadline), `RESOURCE_EXHAUSTED`, `CANCELLED`, `UNKNOWN`, OK-ama-last-yok (INTERNAL) ve gateway'in kendi first-chunk/idle timeout'ları yeniden denenmez. `last` chunk alındıktan sonra hiçbir koşulda retry yapılmaz. `max-attempts=0` retry'ı kapatır.
- **AC6 (durdurma, kapasite):** Şu olaylar bekleyen backoff'u iptal eder ve sonrasında yeni çağrı başlatılmaz: shutdown başlaması (`accepting=false`), shutdown zorla iptali, istemci kopması (emitter callback'i ya da yazım hatası) ve her terminal claim. Backoff süresince permit ve lease tutulur. `releaseCapacity()` akış başına tam bir kez çalışır (`CountingLease.closes==1`). Her terminal yoldan sonra `pendingTimers()` 0'a döner. Registry `stop()` süresi mevcut sınırlar içinde kalır.
- **AC7 (tükenme):** Denemeler ya da bütçe tükenince **son** upstream durumuyla tam bir `error` olayı gider (`code=UNAVAILABLE` ve onun açıklaması). Retry için SSE olayı yoktur. Simulator'ın kendi `UNAVAILABLE@3` hatasında: chunk 0,1,2, ardından `"injected FAULT_TYPE_UNAVAILABLE at seq 3"` gelir. `started_total` +4 (1+3), `faulted_total` +4, `cancelled_total` +0 olur, `active_generations` 0'a iner.
- **AC8 (zamanlayıcılar):** Backoff sırasında kurulu olan tek zamanlayıcı backoff zamanlayıcısıdır; idle ve first-chunk ateşlenemez. Her yeni çağrı kendi first-chunk zamanlayıcısını kurar, ama yalnızca kredi > 0 ise; kredi 0 ise ilk kredi verildiğinde kurulur. Backoff ve timeout callback'leri `sendLock` beklemez. Backoff, retry'ı scheduler thread'inde değil bir sanal thread'de başlatır.
- **AC9 (metrik):** `retries_total{reason=restart|resume}` ve `retry_exhausted_total` önceden kayıtlıdır; başlangıçta 0 okunur, `/actuator/metrics/...` 200 döner. `retries_total` başlatılan her retry çağrısında +1 artar. `retry_exhausted_total`, tükenme sonrası `UPSTREAM_ERROR` claim'ini kazanan akış için +1 artar.
- **AC10 (konfigürasyon):** Varsayılanlar: `lab.retry.max-attempts=3` (retry sayısı; toplam çağrı ≤ 4), `initial-backoff=100ms`, `max-backoff=2s`. Relaxed binding çalışır. Uygulama şu değerlerle başlamaz: `max-attempts` 0..10 dışında, backoff süreleri sıfır ya da negatif, `initial-backoff > max-backoff`, `max-backoff ≥ stream-deadline`. Hata mesajı property adını içerir.
- **AC11 (regresyon):** Mevcut 89 gateway ve 12 simulator testi yeşil kalır. `git diff --stat -- proto simulator` boştur. `gateway/src` altında `Thread.sleep` yoktur. `./mvnw -B verify` yeşildir.
- **AC12 (README):** Yeni "Bounded reconnect" bölümü, ayar ve metrik satırları ve "Known limits" güncellemeleri eklenir. Hipotezler hipotez olarak işaretlenir.

## 2. İlgili dosyalar, mevcut yaklaşım ve önerilen değişiklik

### Doğrulanmış gerçekler (kod okundu)
Kök paket: `gateway/src/main/java/io/github/cemharput/streamlab/gateway/`.

- **`stream/StreamController#stream`:** `requestId = UUID`, sonra lease, `SseEmitter(streamDeadline+5s)`, `new StreamBridge(requestId, emitter, registry, lease, streamQueueCapacity, metrics, timeouts)`, `registry.admit`, ardından `newStub(lease.channel()).withDeadlineAfter(streamDeadline).generateTokens(params.toRequest(requestId), bridge)`. `RuntimeException` gelirse `bridge.onError(INTERNAL "failed to start upstream call")`.
- **`stream/StreamBridge`** `ClientResponseObserver`'dır.
  - Alanlar: `volatile requestStream`, `volatile lastSeen`, `volatile Status upstreamEnd`, `wip`, `ArrayBlockingQueue`, `sendLock`, `AtomicReference<StreamOutcome> terminal`.
  - `beforeStart`: `requestStream` yayınlanır, `disableAutoRequestWithInitial(queueCapacity)`, claim varsa cancel, sonra `timer.start(queueCapacity)`.
  - `onNext`: terminal kontrolü, `timer.chunkReceived`, `chunkQueued`, `offer`; başarısızsa `chunkDequeued` ve `overflow`.
  - `onError`/`onCompleted`: `upstreamEnd` set edilir, `timer.upstreamEnded()`, `scheduleDrain`. Upstream sonu, kuyruk boşaldıktan sonra `handleUpstreamEnd` içinde işlenir.
  - `writeChunk`: `request(1)` lock dışında çalışır ve `timer.creditGranted()`'tan sonra gelir.
  - Claim noktalarının hepsi `claim()` üzerinden geçer: COMPLETED (`sendLock` altında), `handleUpstreamEnd`, `overflow`, `cancelForShutdown`, `clientGone`, `timedOut`.
- **`stream/StreamTimer`:** yaprak `timerLock` altında tek `Armed` slotu vardır. Alanlar: `closed`, `firstChunkSeen`, `upstreamDone`, `outstanding`. Metotlar: `start`, `chunkReceived`, `creditGranted`, `upstreamEnded`, `close`. `fire`, token kontrolünden sonra callback'i lock dışında çağırır.
- **`stream/StreamTimeouts`:** `schedule(Runnable, Duration)` durmuşsa `RejectedExecutionException` fırlatır. `pendingTimers()` kuyruk boyunu verir (`removeOnCancel`). Faz −768.
- **`stream/ActiveStreamRegistry`:** `isAccepting()` public'tir ve lock'u kısa süre alır. `stop()`: `accepting=false`, `awaitEmpty(drain)`, `cancelAll`, 2 s.
- **`config/GatewayProperties`:** beş bileşen ve üst seviye compact constructor (`requireShorterThanDeadline`). Repoda tek `new GatewayProperties(` çağrısı vardır: `StreamTestSupport.properties` (grep ile doğrulandı). Tek `new StreamBridge(` test çağrısı `StreamTestSupport.newBridge`'tedir.
- **Simulator:**
  - `Generation` `nextSeq = resume_from_seq` ile başlar.
  - Fault, `at_seq == nextSeq` olduğunda ateşlenir; resume edilen çağrıda tekrar ateşlenir.
  - `first_chunk_delay` resume'da da uygulanır (proto yorumu).
  - `RequestLimits` `resume_from_seq ≥ chunk_count` ise `INVALID_ARGUMENT` döner.
  - Chunk metni `(seed, requestId.hashCode(), seq)` fonksiyonudur; küçük harf kelimelerdir, JSON kaçışı gerekmez.
  - İstemci tarafında başarısız olan çağrı simulator'a ulaşmaz, `started_total` artmaz.
- **Testler:**
  - `StreamEndToEndTest#unavailableMidStream` (`UNAVAILABLE@3`, `atMost 10s`) yeni varsayılanlarla da geçer. 3 retry yapılır, her biri yeniden ateşlenir; ek süre en fazla ~0,7 s'dir. Mesaj aynıdır.
  - `StreamBridgeTest` ve `StreamBridgeTimeoutTest`, köprüyü doğrudan observer olarak kullanır: `bridge.beforeStart` / `onNext` / `onError` toplam 57 çağrı. `UpstreamFlowControlTest` `generateTokens(request, bridge)` yapar.
  - `InProcessSimulatorConfig#inProcessChannelFactory` `@Primary`'dir.

### Tasarım kararları

**D1. Yapı: köprü akışın sahibi kalır; her çağrı bir `Attempt` olur.**
- `StreamBridge` içinde `private final class Attempt implements ClientResponseObserver<GenerateRequest, TokenChunk>` tanımlanır.
  - Alanlar: `final int number` (1'den başlar), `volatile ClientCallStreamObserver<GenerateRequest> call`, `final AtomicBoolean ended`, `AttemptState state` ve `int banked`. `state` ve `banked` `creditLock` altındadır.
  - Durumlar: `NEW`, `STARTING`, `OPEN`, `DEAD`.
  - Callback'ler köprüye delege edilir:
    - `beforeStart` → `attemptBeforeStart(this, s)`.
    - `onNext` → `if (current != this) return; chunkFromUpstream(this, c)`.
    - `onError(t)` / `onCompleted()` → `if (ended.compareAndSet(false, true)) attemptEnded(this, status)`.
  - Bir başarısız denemenin geç callback'i (`current != this` ya da `ended`) yok sayılır.
- `volatile ClientCallStreamObserver requestStream` alanının yerini `volatile Attempt current` alır. Constructor `current = initialAttempt` (number 1) kurar.
- **Geriye uyum:** `StreamBridge implements ClientResponseObserver` kalır. Dört metodu `initialAttempt`'e delege eder. Böylece 57 test çağrısı ve `UpstreamFlowControlTest` değişmez (attempt 1 yolu). Testlerde varsayılan `RetryPolicy.disabled()` kullanılır.
- `cancelUpstream(msg)`, `current.call` değerini okur ve null değilse iptal eder. `clientGone`, `cancelForShutdown`, `timedOut` ve `overflow` yalnızca güncel denemeye etki eder. Ölü bir çağrıda cancel no-op'tur.
- **Claim ile yeni denemenin yarışı (Dekker deseni):**
  - Başlatan taraf: `current = a`, ardından `beforeStart` içinde `a.call = s`, ardından `terminal` okunur; claim varsa `s.cancel`.
  - Claim eden taraf: CAS, ardından `current` ve `current.call` okunur.
  - Volatile toplam sıra nedeniyle iki taraftan biri diğerini görür. Ayrıca `startAttempt` başında `terminal` kontrol edilir. İptal edilmiş bir çağrıda `call.start` fırlatırsa `startCall` içindeki catch yakalar; terminal zaten claim edilmiş olduğu için zararsızdır.
- **Retry kararı** `attemptEnded(a, status)` içinde, o denemenin seri gRPC thread'inde verilir. Retry ancak şu koşulların hepsi doğruysa yapılır:
  1. `terminal == null`,
  2. kod `UNAVAILABLE`,
  3. `!lastReceived`,
  4. `a.number <= policy.maxRetries()`,
  5. `registry.isAccepting()`,
  6. `delay < budgetDeadline − now`.

  Hepsi doğruysa:
  - `creditLock` altında `a.state = DEAD` ve `timer.callEnded()`.
  - `lastStatus = status`.
  - Log yazılır.
  - `timer.scheduleRetry(delay, () -> RETRY_EXECUTOR.execute(() -> startAttempt(a.number + 1)))`. `false` dönerse `recordEnd(status)` çağrılır.

  Aksi halde: koşul 4 ya da 6 nedeniyle düştüyse ve `maxRetries > 0` ise `retriesExhausted = true` olur ve log yazılır. Sonra `recordEnd(status)` çağrılır; bu, eski `onError` gövdesidir: `upstreamEnd` set edilir, `timer.upstreamEnded()`, `scheduleDrain`.
- **Retryable `UNAVAILABLE`, `upstreamEnd`'i atlar.** Drainer kuyruğu boşaltmaya devam eder ve `upstreamEnd == null` olduğu için akışı bitirmez; backoff, boşaltmayla örtüşür. Sıra şöyle korunur: eski çağrının bütün `offer`'ları onun `onError`'ından önce olur (seri callback). Yeni çağrı ancak ondan sonra başlar ve aynı FIFO kuyruğa yazar.
- **Retry hiçbir zaman callback içinden inline başlatılmaz.** Her zaman scheduler ve ardından sanal thread `stream-retry` üzerinden geçer. Gerekçeler: başlatma içinde senkron gelen bir `onError` sonsuz özyinelemeye yol açmaz; gRPC callback thread'i ve scheduler thread'i bloklanmaz.
- **`startAttempt(n)`** (sanal thread):
  - `terminal != null` ise döner.
  - `!registry.isAccepting()` ise log yazılır ve `recordEnd(lastStatus)`; bu bir tükenme sayılmaz.
  - `remaining = budgetDeadline − now`; `≤ 0` ise `retriesExhausted = true` ve `recordEnd(lastStatus)`.
  - Aksi halde `a = new Attempt(n)`, `current = a`, `metrics.retried(nextSeq == 0 ? "restart" : "resume")`, `startCall(a, Duration.ofNanos(remaining))`.
- **`startCall(a, deadline)`** hem attempt 1 hem retry'larda kullanılır:
  - `try { starter.start(requestFor(nextSeq), deadline, a); } catch (RuntimeException ex) { a.fail(INTERNAL "failed to start upstream call") }`. `a.fail`, `ended` CAS'ını yapar ve retryable değildir.
  - Ardından `markStarted(a)`.
- **`start()`** (public değil, package-private): `budgetDeadline = policy.nanoTime() + streamDeadline.toNanos()`, sonra `startCall(initialAttempt, streamDeadline)`. Controller'daki catch bloğu buraya taşınır.

**D2. Resume pozisyonu: son ALINAN seq + 1 (isteğin metninden bilinçli sapma).**
- İstek "son yazılan seq + 1" diyor. Ancak kuyruktaki alınmış ve henüz yazılmamış chunk'lar drainer tarafından yine yazılacak. Son yazılan + 1'den devam etmek onları **tekrar** gönderir ve isteğin "tekrar olmaz" şartını bozar. Diğer seçenek kuyruğu atıp son yazılandan devam etmektir; bu veriyi boşa atar, kuyruk ameliyatı ve ek yarışlar getirir. Seçilen yol, istemcinin gördüğü sonuçta isteğin niyetiyle aynıdır: istemcinin son gördüğü id, drain bittikten sonra son alınan seq olur.
- İkinci sapma: chunk alınmış ama hiç yazılmamışsa (hepsi kuyruktaysa) akış baştan başlamaz, alınanın arkasından devam eder. İkisi de README'de belirtilir.
- **İzleme:**
  - `volatile int nextSeq` (başlangıç 0) ve `volatile boolean lastReceived` alanları `chunkFromUpstream` içinde, `offer`'dan önce yazılır: `nextSeq = chunk.getSeq() + 1`, `if (chunk.getLast()) lastReceived = true`.
  - Tek yazar, güncel denemenin seri gRPC thread'idir. Retry thread'i bu alanları ancak o denemenin `onError`'ından sonra okur; happens-before, timer zamanlaması ve volatile üzerinden sağlanır.
  - Seq sürekliliği zorlanmaz. Mevcut testlerde atlamalı seq teslimi olabilir; bu bir risk olarak not edilir (R5).
- **İstek sahipliği:** Controller temel isteği kurar (`params.toRequest(requestId)`) ve köprüye verir. Köprü `requestId` değerini `request.getRequestId()`'den alır. `requestFor(seq)`: `seq == 0` ise temel istek (resume set edilmez), değilse `request.toBuilder().setResumeFromSeq(seq).build()`. `request_id`, `seed`, `profile` ve `fault` aynen kalır; içerik byte düzeyinde aynıdır ve simulator fault'u `at_seq`'te yeniden ateşlenir.
- `lastReceived` iken retry yapılmadığı için `resume_from_seq ≤ chunk_count − 1` her zaman sağlanır. Bu, simulator'ın `< chunk_count` doğrulamasıyla uyumludur.

**D3. Kredi ve kuyruk.**
- Yeni yaprak-öncesi `ReentrantLock creditLock` ve onun altında `int buffered` eklenir. `buffered`, `chunkFromUpstream`'in kabul ettiği ve kredisi henüz iade edilmemiş chunk sayısıdır: kuyrukta + yazımda + yazılmış ama iade edilmemiş.
- Lock sırası: `creditLock → timerLock` ve `sendLock → timerLock`. `creditLock` ile `sendLock` iç içe alınmaz; iade `sendLock` bırakıldıktan sonra yapılır.
- **Kurallar:**
  - `chunkFromUpstream`: `creditLock` altında `buffered++`, sonra `chunkQueued()` ve `offer` (sıra korunur).
  - `attemptBeforeStart(a, s)`:
    - `a.call = s`.
    - `creditLock` altında: `initial = capacity − buffered`; `a.state = (a.number == 1 ? OPEN : STARTING)`; `timer.start(initial)`.
    - Sonra `s.disableAutoRequestWithInitial(initial)` (yalnızca `beforeStart`'ta geçerlidir).
    - Sonra claim kontrolü yapılır ve gerekirse `s.cancel`.
    - Attempt 1'de `buffered == 0` olduğundan `initial == capacity` olur; bugünkü davranış ve `initialRequest` testleri değişmez.
  - `handBackCredit()` (drainer, başarılı ve `last` olmayan yazımdan sonra, lock dışında):
    - `creditLock` altında `buffered--`; `terminal != null` ise döner.
    - `cur = current`; `OPEN` ise `timer.creditGranted(1)` ve `send = true`; `STARTING` ise `cur.banked++`; `NEW`/`DEAD` ise hiçbir şey yapılmaz (kredi havuza döner).
    - Lock dışında, `send` ise `cur.call.request(1)`.
  - `markStarted(a)` (`starter.start` döndükten sonra): `creditLock` altında `STARTING` ise `OPEN` yapılır, `n = banked`, `banked = 0`, `n > 0` ise `timer.creditGranted(n)`. Lock dışında `n > 0` ise `a.call.request(n)`.
  - `attemptEnded` retry yolu: `creditLock` altında `a.state = DEAD` ve `timer.callEnded()`.
- **Gerekçe:**
  - `beforeStart` ile `call.start` arasında çağrılan `request(1)`, grpc'de büyük olasılıkla "Not started" ile fırlatır (doğrulanacak varsayım A1). Banka bunu önler.
  - Attempt 1'de bu pencere yoktur, çünkü çağrı başlamadan chunk olamaz.
  - Timer güncellemesi `creditLock` altında yapılır. Böylece A denemesine ait bir iade, B denemesinin `outstanding` değerine karışamaz. Karışsaydı idle zamanlayıcı kredi 0 iken kurulabilirdi.
- **Değişmez:** OPEN güncel çağrı için `outstanding + buffered + banked == capacity`. Ölen çağrının kalan kredisi düşer; gRPC `onClose`'dan sonra `onNext` teslim etmez, ayrıca `current != this` koruması vardır. `queue.size() ≤ buffered ≤ capacity` olduğu için iyi davranan bir upstream'de `offer` başarısız olmaz.
- `chunkQueued`/`chunkDequeued`/`discardQueued` muhasebesi değişmez. Geçişte kuyruk korunur; discard yalnızca terminalde yapılır. `buffered` terminalden sonra önemsizdir.
- Test erişimcisi: package-private `int bufferedChunks()`.

**D4. Zamanlayıcılar.**
- **First-chunk zamanlayıcısı her deneme için ayrı kurulur.** Gerekçeler:
  - Simulator `first_chunk_delay`'i resume'da da uygular.
  - Akış başına tek bir first-chunk zamanlayıcısı olsaydı, ikinci denemenin meşru TTFT'si idle limitine takılabilirdi.
  - Toplam süre zaten bütçeyle sınırlıdır.
  - Bedeli: `timeouts_total{first_chunk}` resume edilen çağrının TTFT'sini de sayar (README'de belirtilir).
- **`StreamTimer` değişiklikleri:**
  - Yeni `boolean callActive`.
  - `Armed` sınıfına `Runnable retryTask` eklenir; timeout'larda null'dır.
  - `start(int credit)` çağrı başına sıfırlar: `outstanding = credit`, `firstChunkSeen = false`, `callActive = true`, `disarm()`, `!closed && !upstreamDone && credit > 0` ise FIRST_CHUNK kurulur. Attempt 1'de `credit = capacity ≥ 1` olduğundan davranış aynıdır.
  - `creditGranted(int n)`: no-arg sürüm 1'e delege eder. `outstanding += n`; `!closed && !upstreamDone && callActive && armed == null && outstanding > 0` ise `firstChunkSeen` değerine göre IDLE ya da FIRST_CHUNK kurulur.
  - `chunkReceived`'de kurma koşuluna `callActive` eklenir.
  - Yeni `callEnded()`: `callActive = false` ve `disarm()`; `upstreamDone` değişmez.
  - Yeni `boolean scheduleRetry(Duration, Runnable)`: lock altında `closed` ise `false`; değilse `disarm()`, yeni token ve `schedule`. `RejectedExecutionException` gelirse `false` döner.
  - `fire`: token kontrolünden sonra `retryTask != null` ise `retryTask.run()` çağrılır; bu yalnızca sanal thread'e devreder. Değilse `onExpiry`.
  - Yeni accessor: `boolean retryPending()`.
- **Backoff sırasında:** çağrı yoktur, `callActive = false` olduğu için idle kurulamaz ve tek slotta backoff token'ı durur. Pozitif kontrol: `pendingTimers() == 1`.
- `close()` (her claim) backoff'u da iptal eder; bu yüzden sızıntı yoktur. `StreamTimeouts`'ta API değişmez, yalnızca javadoc güncellenir: `pendingTimers` artık backoff'u da sayar.
- Hiçbir zamanlayıcı callback'i `sendLock`, `creditLock` ya da registry lock'u beklemez. `timedOut` değişmez.

**D5. Politika ve bütçe.**
- **Konfigürasyon:** `GatewayProperties.Retry(@DefaultValue("3") int maxAttempts, @DefaultValue("100ms") Duration initialBackoff, @DefaultValue("2s") Duration maxBackoff)`.
  - `MAX_ATTEMPTS = 10`.
  - Compact doğrulama: `maxAttempts` 0..10 aralığında olmalı (mesaj "lab.retry.max-attempts must be between 0 and 10"); iki süre de `requirePositive`; `initial > max` ise "lab.retry.initial-backoff (..) must not exceed lab.retry.max-backoff (..)".
  - `defaults()` ve `disabled()` (0, 100ms, 2s) eklenir.
  - Üst seviye compact constructor'a `requireShorterThanDeadline(retry.maxBackoff(), "lab.retry.max-backoff", deadline)` eklenir.
  - **`max-attempts` retry sayısıdır, toplam çağrı sayısı değildir.** İstek "max-attempts kadar yeniden dene" diyor; toplam çağrı ≤ max-attempts + 1. Bu, gRPC service-config'teki `maxAttempts`'tan (toplamı sayar) farklıdır ve README'de vurgulanır. `0` retry'ı kapatır. Üst sınır 10'dur: sınırlı kaynak ilkesi.
- **Yeni `stream/RetryPolicy`** (package-private final):
  - `RetryPolicy(int maxRetries, Duration initial, Duration max, Jitter jitter, LongSupplier nanoTime)` ve `static RetryPolicy from(GatewayProperties.Retry)`. Bu fabrika `Jitter.RANDOM` (`b -> ThreadLocalRandom.current().nextLong(b + 1)`) ve `System::nanoTime` kullanır.
  - `static disabled()`.
  - `boolean isRetryable(Status s)`: `s.getCode() == UNAVAILABLE`.
  - `long ceilingNanos(int k)`: `initial << k` ile hesaplanır ve `max`'a doyurulur. `k ≥ 63` ya da kaydırma taşarsa `max` döner.
  - `Duration backoff(int k)`: `Duration.ofNanos(jitter.draw(ceilingNanos(k)))`.
  - `maxRetries()`, `nanoTime()`.
  - `@FunctionalInterface interface Jitter { long draw(long boundInclusiveNanos); }`.
  - Spring bean değildir; controller kurar.
- **Yeni `stream/UpstreamStarter`** (package-private `@FunctionalInterface`): `void start(GenerateRequest request, Duration deadline, ClientResponseObserver<GenerateRequest, TokenChunk> observer)`. Controller'daki gerçeklemesi: `(r, d, o) -> TokenStreamServiceGrpc.newStub(lease.channel()).withDeadlineAfter(d.toNanos(), NANOSECONDS).generateTokens(r, o)`.
- **Bütçe:** retry kararında `delay ≥ remaining` ise retry yapılmaz ve son durum raporlanır. Başlatma anında `remaining ≤ 0` ise yine raporlanır. Her çağrının deadline'ı kalan bütçedir. Kalan bütçe first-chunk ya da idle limitinden kısaysa önce gRPC `DEADLINE_EXCEEDED` gelir; bu retry edilmez. Saat yalnızca bütçe aritmetiğinde kullanılır; gerçek bekleme `StreamTimeouts` scheduler'ı ile yapılır.

**D6. Yaşam döngüsü ve shutdown.**
- **Drain sırasında (`accepting = false`) yeni retry yapılmaz.**
  - Karar anında `isAccepting()` false ise retry yapılmaz, hata son durumla iletilir.
  - Backoff'tayken shutdown başlarsa `startAttempt` bunu görür, çağrı başlatmaz ve son durumu (UNAVAILABLE) raporlar.
  - Gerekçeler: istek "shutdown retry'ı durdurur" diyor; drain yakınsamalı; drain sırasındaki UNAVAILABLE büyük olasılıkla shutdown'ın kendisinden kaynaklanır.
  - Bedeli: drain içinde kurtarılabilecek bir akış `UNAVAILABLE` ile biter (README'de belirtilir).
  - Drain sınırı her durumda korunur. Backoff'taki akış tracked'tır; `cancelAll` onu claim eder ve `close()` backoff'u iptal eder.
- Zorla iptal, istemci kopması, kuyruk yazım hatası ve timeout, mevcut claim yollarıyla çalışır. `claim()` timer'ı kapatır, dolayısıyla backoff da iptal olur. Ölü çağrıda cancel no-op'tur. `startAttempt` terminal görür ve hiçbir şey başlatmaz.
- Permit, backoff boyunca tutulur; `releaseCapacity` yalnızca claim kazananında çağrılır.
- Faz sırası değişmez: registry (−512) önce durur ve tracked her akış claim edilir; sonra `StreamTimeouts` (−768) durur. Yarıda kalmış bir `stream-retry` sanal thread'i terminal görür ve çıkar. Scheduler durmuşsa `scheduleRetry` `false` döner ve `recordEnd` çağrılır.
- `LifecyclePhases` ve `ActiveStreamRegistry` değişmez.

**D7. Görünür davranış ve log.**
- İstemci, retry hakkında hiçbir ipucu almaz; chunk id'leri süreklidir ve tek bir terminal olay vardır.
- Hata metni mevcut `handleUpstreamEnd(status)` formatıyla gider: `code` ve `description`.
- INFO log satırları:
  - "Stream {0}: attempt {1} failed with {2}; retry {3}/{4} in {5} ms from seq {6}"
  - "Stream {0}: retries exhausted after {1} attempt(s) ({2}: attempts|budget)"
  - "Stream {0}: retry stopped, gateway shutting down"
  - Mevcut "Stream {0} finished: …" satırı aynen kalır.

**D8. Metrikler (`StreamMetrics`).**
- `RETRY_REASONS = List.of("restart", "resume")` olur. Neden her zaman UNAVAILABLE olduğu için `reason`, retry'ın baştan mı yoksa resume ile mi yapıldığını ayırır; laboratuvar için asıl bilgi budur.
- Önceden kayıtlı metrikler: `retries_total{reason}` ve tag'siz `retry_exhausted_total`.
- Metotlar: `retried(String)` ve `retryExhausted()`.
- Artıranlar:
  - `retries_total`: `startAttempt`, yeni çağrıyı başlatmadan hemen önce, tek bir thread'de artırır. Böylece değer başlatılan ek çağrı sayısına eşittir.
  - `retry_exhausted_total`: `handleUpstreamEnd` içinde `UPSTREAM_ERROR` claim'ini kazanan, `retriesExhausted` ise artırır. Bu alan volatile'dır ve `upstreamEnd` yazımından önce set edilir. Böylece bir akış en fazla bir kez sayılır. `max-attempts = 0` ve shutdown durdurması sayılmaz.

### Deneme yaşam döngüsü (durum tablosu)

| Akış fazı | Giriş | Kurulu zamanlayıcı | Kredi iadesi | Çıkış |
|---|---|---|---|---|
| CALL(n) STARTING (n ≥ 2) | `attemptBeforeStart` | kredi > 0 ise FIRST_CHUNK | `banked++` | `markStarted`, ardından OPEN |
| CALL(n) OPEN | `beforeStart` (n = 1) ya da `markStarted` | FIRST_CHUNK, sonra IDLE (mevcut kurallar) | `request(1)` | retryable son ise BACKOFF; değilse ENDING; claim ise TERMINAL |
| BACKOFF(n) | `attemptEnded` + `scheduleRetry` | yalnızca backoff | yok (havuza döner) | ateşlenir, `startAttempt(n+1)`; reddedilirse ENDING; claim ise TERMINAL |
| ENDING | `recordEnd(status)` | yok (`upstreamEnded`) | (eski davranış) | drainer kuyruğu boşaltır, sonra `handleUpstreamEnd` ile TERMINAL |
| TERMINAL | `claim()` | yok (`close`) | yok | — |

### Etkileşim matrisi (yeni satırlar; mevcut D5 matrisi güncel çağrı için aynen geçerlidir)

| Olay | BACKOFF sırasında | STARTING penceresinde (n ≥ 2) |
|---|---|---|
| Upstream chunk | Mümkün değil (çağrı yok); ölü denemeden gelirse `current != this` ile yok sayılır | Normal `onNext`, `buffered++` |
| Upstream error/complete | Ölü denemeden gelirse `ended`/`current` ile yok sayılır | Normal karar (yeniden BACKOFF ya da ENDING); `markStarted` DEAD görür, no-op |
| İstemci gitti (callback ya da kuyruk yazım hatası) | CLIENT_GONE claim'i, `close` backoff'u iptal eder, çağrı başlamaz, `closes == 1` | Claim, ardından Dekker deseniyle yeni çağrı iptal edilir |
| Shutdown başladı (`accepting = false`) | `startAttempt` çağrı başlatmaz; kuyruk yazılır, ardından `error` UNAVAILABLE (son durum) | Çağrı sürer; sonraki UNAVAILABLE retry edilmez |
| Shutdown zorla iptal | SHUTDOWN claim'i; `error` CANCELLED; backoff iptal | Claim ve yeni çağrının iptali |
| First-chunk/idle timeout | Kurulu değil, ateşlenemez | Kredi > 0 ise FIRST_CHUNK kurulu; ateşlenirse claim, retry yok |
| Last chunk | BACKOFF'a girilemez (`lastReceived`) | Normal COMPLETED |
| Overflow | Mümkün değil | `initial = capacity − buffered` olduğu için iyi davranan upstream'de olmaz; olursa mevcut yol |
| Backoff ateşlenir, aynı anda claim | Token kontrolü ya da `startAttempt`'teki terminal kontrolü yakalar; geçmişse Dekker deseni | — |
| Bütçe biter | Kararda ya da başlatmada; `error` son durum, `retry_exhausted` +1 | gRPC deadline = kalan bütçe; `DEADLINE_EXCEEDED` retry edilmez |
| Scheduler durmuş | `scheduleRetry` false, ardından `recordEnd(status)` | — |

## 3. Uygulama adımları

1. `config/GatewayProperties.java`: `Retry` record'u, altıncı bileşen `@DefaultValue Retry retry` ve üst seviye `max-backoff` kontrolü (D5). `application.yaml`'a `lab.retry: {max-attempts: 3, initial-backoff: 100ms, max-backoff: 2s}` bloğu eklenir.
2. Yeni dosyalar: `stream/RetryPolicy.java` ve `stream/UpstreamStarter.java` (D5).
3. `stream/StreamTimer.java`: `callActive`, `start` sıfırlaması, `creditGranted(int)`, `callEnded`, `scheduleRetry`, `fire` dalı, `retryPending` (D4). Lock yaprak lock olarak kalır.
4. `stream/StreamMetrics.java`: iki yeni metrik (D8) ve javadoc.
5. `stream/StreamBridge.java`:
   - Yeni constructor: `(GenerateRequest request, SseEmitter, ActiveStreamRegistry, ChannelProvider.Lease, int queueCapacity, StreamMetrics, StreamTimeouts, RetryPolicy, UpstreamStarter, Duration streamDeadline)`.
   - `Attempt` iç sınıfı, `current`, `creditLock`/`buffered`, `nextSeq`/`lastReceived`/`lastStatus`/`retriesExhausted`/`budgetDeadline`.
   - Metotlar: `start()`, `startCall`, `markStarted`, `attemptBeforeStart`, `chunkFromUpstream` (eski `onNext` gövdesi + `buffered`/`nextSeq`), `attemptEnded`, `recordEnd`, `startAttempt`, `handBackCredit`, `RETRY_EXECUTOR`.
   - `handleUpstreamEnd`'e tükenme sayacı eklenir.
   - `cancelUpstream` `current` üzerinden çalışır.
   - Erişimciler: `bufferedChunks()`, `retryPending()`.
   - Javadoc'a deneme paragrafı eklenir.
   - Dört public observer metodu `initialAttempt`'e delege edilir.
6. `stream/StreamController.java`: `RetryPolicy.from(properties.retry())` alana alınır. Bridge temel istek ve starter lambda'sıyla kurulur. Admission'dan sonra `bridge.start()` çağrılır; `try/catch` kaldırılır.
7. Test desteği:
   - `stream/StreamTestSupport`:
     - `properties(...)` `Retry.defaults()` geçer.
     - `newBridge(...)` aşırı yüklemeleri yeni constructor'ı `GenerateRequest(requestId "r")`, `RetryPolicy.disabled()`, `NO_STARTER` (fırlatır) ve 60 s ile kurar.
     - Yeni `newRetryingBridge(emitter, registry, lease, capacity, metrics, timeouts, RetryPolicy, UpstreamStarter, Duration deadline)`.
     - Yeni `FakeUpstreamStarter implements UpstreamStarter`. `record Call(GenerateRequest request, Duration deadline, ClientResponseObserver<GenerateRequest,TokenChunk> observer, FakeUpstream upstream)` tanımlanır. `List<Call> calls`, `volatile Consumer<Call> onStart` ve `start()` vardır. `start()` her çağrıda yeni bir `FakeUpstream` oluşturur, `observer.beforeStart(up)`'ı senkron çağırır, sonra `onStart` çalışır. Bu kanca senkron `onError` ya da latch ile bekleme için kullanılır.
     - `FakeUpstream#deliver(ClientResponseObserver<?, TokenChunk>, TokenChunk)` aşırı yüklemesi eklenir.
     - `RecordingEmitter` `onCompletion` metodunu override eder (super + yakalama) ve `simulateClientGone()` sunar. `failSends` volatile ve ayarlanabilir olur.
     - `fixedClock` için `AtomicLong` yardımcısı eklenir.
   - Yeni `testsupport/FaultInjectingInterceptor` (public, `ClientInterceptor`), yalnızca `TokenStreamServiceGrpc.getGenerateTokensMethod()` için:
     - `failNextCalls(int n)`: sonraki n çağrı için `ClientCall` alt sınıfı döner. `start`, `listener.onClose(UNAVAILABLE "injected by test interceptor: call refused", new Metadata())` olayını bir sanal thread'de teslim eder. `request`, `cancel`, `halfClose`, `sendMessage` no-op'tur. Çağrı simulator'a ulaşmaz.
     - `cutNextCalls(int n, int afterChunks)`: `ForwardingClientCall.SimpleForwardingClientCall` + `ForwardingClientCallListener.SimpleForwardingClientCallListener` kullanılır. K'ıncı mesaj iletildikten sonra `cut = true` ve `delegate().cancel("test cut", null)`. Sonraki mesajlar düşürülür. `onClose` `cut` ise `UNAVAILABLE "injected by test interceptor: cut after K chunks"` ile değiştirilir.
     - `interceptedCalls()`, `reset()`. Plan yoksa geçirgendir.
   - `testsupport/InProcessSimulatorConfig`:
     - `faults` alanı ve `public FaultInjectingInterceptor faults()`.
     - Kanal fabrikası `InProcessChannelBuilder.forName(name).intercept(faults).build()`.
     - `public List<String> referenceTexts(String requestId, long seed, int chunks)`: taze ve interceptor'süz kanalda blocking stub kullanır, interval 0'dır.
8. Testler (Bölüm 4).
9. `README.md` (AC12):
   - Durum satırı ve modül tablosu güncellenir.
   - "Timeouts and cancellation"dan sonra yeni "Bounded reconnect" bölümü. İçerik:
     - ayar tablosu ve `max-attempts` = retry sayısı;
     - tam jitter formülü;
     - bütçe ve kalan deadline;
     - resume pozisyonu kararı ve isteğin metninden sapma;
     - yeniden denenmeyenler (timeout'lar dahil);
     - shutdown/drain kuralı;
     - SSE ipucu olmadığı;
     - first-chunk zamanlayıcısının her denemede kurulduğu.
   - Metrik ve ayar tablolarına satırlar eklenir.
   - "Known limits":
     - gerçek bir LLM resume ve determinizm desteklemeyebilir; laboratuvar deterministik simulator'a dayanır;
     - kısmi çıktıdan sonra gelen UNAVAILABLE upstream işini tekrarlatabilir;
     - drain sırasında kurtarılabilecek akış UNAVAILABLE ile biter;
     - istemci kopmasının backoff sırasında (yazım yokken) Tomcat'te algılanması doğrulanmamıştır (hipotez);
     - per-request lease'in retry'lar boyunca yaşaması gerekir;
     - jitter 0 olabilir;
     - emitter zaman aşımı admission beklemesini de içerir (mevcut).
   - `pendingTimers` backoff'u da sayar.
10. Değişmeyecek dosyalar: `proto/`, `simulator/`, `pom.xml`, `upstream/*`, `ActiveStreamRegistry`, `LifecyclePhases`.

## 4. Doğrulama

Genel kurallar:
- Tüm yeni sınıflarda `@Timeout(threadMode = SEPARATE_THREAD)` olur: unit 30, e2e 60.
- Gate, latch ve semaphore'lar `finally` içinde açılır.
- Başlatılan her `StreamTimeouts` `@AfterEach` içinde durdurulur.
- `Thread.sleep` yoktur.
- Her `during(X)` ilgili zamanlayıcının en az 5 katıdır. Her `atMost`, beklenen sürenin en az 5 katıdır.
- Unit testlerde deterministik jitter kullanılır (`b -> 0`, `b -> b` ya da initial=max ile sabit). Bütçe testlerinde `AtomicLong` saati kullanılır.

| AC | Test (sınıf#metot) | Beklenen |
|---|---|---|
| AC3 | **Yeni** `stream/RetryPolicyTest#ceilingsDoubleUpToMax`, `#jitterStaysWithinZeroAndCeiling` (RANDOM ile her k için 1000 çekim), `#injectedJitterReceivesTheCeiling`, `#hugeExponentSaturatesAtMax` (k=100), `@ParameterizedTest(EnumSource Status.Code) #onlyUnavailableIsRetryable`, `#disabledHasNoRetries` | Tavanlar 100, 200, 400, 800, 1600, 2000, 2000 ms; her çekim [0, tavan] içinde; yalnızca UNAVAILABLE true |
| AC1 | **Yeni** `stream/StreamBridgeRetryTest#unavailableBeforeFirstChunkRestartsWithoutResume` (jitter 0) | 2 çağrı; 2. isteğin `getResumeFromSeq() == 0`, `request_id` ve `seed` aynı, `initialRequest == capacity`; teslimden sonra `complete`; `closes == 1`; `retries{restart} == 1`; `pendingTimers()` 0 |
| AC1 | `StreamBridgeRetryTest#immediateFailureInsideStartIsRetried` (`onStart` ilk 2 çağrıda senkron `onError(UNAVAILABLE)`) | 3 çağrı, özyineleme yok, `complete`, `retries{restart} == 2` |
| AC2 | `StreamBridgeRetryTest#midStreamResumeStartsAfterLastReceivedNotLastWritten` (capacity 8, stalled emitter: chunk0 yazımda takılı, 1-2 kuyrukta; `onError(UNAVAILABLE)`) | 2. istek `resume_from_seq == 3`; `initialRequest == 5` (`8 − buffered 3`); gate açılınca 3..5(last) teslim edilir; olaylar 0..5 + `complete`, tekrar yok; `retries{resume} == 1` |
| AC2 | `StreamBridgeRetryTest#creditFreedWhileTheNewCallStartsIsBankedThenFlushed` (`onStart` bir latch'te bekler) | Latch kapalıyken gate açılır ve chunk0 yazılır; `fake2.requested == 0`; latch açılınca `requested == 1`; `initialRequest + requested == capacity − bufferedChunks()` |
| AC2 | `StreamBridgeRetryTest#lateCallbacksOfAFailedAttemptAreIgnored` | Eski observer'a `onNext`/`onError`/`onCompleted`: olay yok, `bufferedChunks` değişmez, ek çağrı yok |
| AC2 (gerçek gRPC) | **Genişleyen** `UpstreamFlowControlTest#resumedCallWithNoFreeCreditWaitsForTheClient` (capacity 2, 50 chunk, interval 1 ms, `cutNextCalls(1, 2)`, stalled emitter, jitter 0, starter interceptor'lü kanal) | Taşma yok; `started_total == 2`, `cancelled_total == 1`; gate açılınca 0..49 sırayla + tek `complete`; `active_generations == 0`; `pendingTimers()` 0. Başlangıç kredisi 0 ve `request` "Not started" varsayımlarını kanıtlar |
| AC2, AC1 (e2e) | **Yeni** `gateway/RetryAndResumeTest#cutStreamResumesWithoutDuplicateOrGap` (`cutNextCalls(2, 3)`, `chunks=10&intervalMs=10`, `--lab.retry.initial-backoff=50ms --lab.retry.max-backoff=200ms`) | `chunkIds` tam olarak "0".."9"; tek `complete` ve `"chunks":10`; `started` +3, `cancelled` +2, `completed` +1, `active` 0; `retries{resume} == 2`; metinler `referenceTexts(X-Request-Id, 0, 10)` ile birebir aynı |
| AC1 (e2e) | `RetryAndResumeTest#refusedCallsAreRetriedAndTheStreamCompletes` (`failNextCalls(2)`) | `complete`, 0..4; `started_total` +1; `retries{restart} == 2`; `exhausted == 0`; `active_streams == 0`; `pendingTimers()` 0 |
| AC7 (e2e) | `RetryAndResumeTest#simulatorUnavailableFaultExhaustsRetries` (`UNAVAILABLE@3`, chunks 10) | 0,1,2 + tek `error` UNAVAILABLE "injected FAULT_TYPE_UNAVAILABLE at seq 3"; `started` +4, `faulted` +4, `cancelled` +0, `active` 0; `retries{resume} == 3`; `retry_exhausted_total == 1` |
| AC7 | `RetryAndResumeTest#refusedCallsExhaustTheConfiguredAttempts` (`--lab.retry.max-attempts=2`, `failNextCalls(100)`) | Tek `error` UNAVAILABLE + interceptor açıklaması; `started_total` +0; `retries{restart} == 2`; `exhausted == 1` |
| AC7 | `StreamBridgeRetryTest#exhaustionSendsOneErrorWithTheLastStatus` (`maxRetries` 2, her çağrıda "boom-n") | 3 çağrı; tek `error` içinde "boom-3"; `closes == 1`; `exhausted == 1`; `during(1s)` boyunca ek olay ya da çağrı yok |
| AC5 | `StreamBridgeRetryTest @ParameterizedTest #nonRetryableCodesAreNotRetried` (INTERNAL, INVALID_ARGUMENT, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, CANCELLED, UNKNOWN); `#firstChunkTimeoutIsNotRetried` (first-chunk 300 ms); `#lastChunkReceivedIsNeverRetried`; `#zeroMaxAttemptsDisablesRetry`; `#starterFailureIsInternalAndNotRetried`; e2e `RetryAndResumeTest#internalFaultIsNotRetried` | 1 çağrı; ilgili kodla tek `error` (last durumunda `complete`); `retries` 0; `exhausted` 0; timeout testinde `during(1.5s)` (5x) boyunca ek çağrı yok |
| AC4 | `StreamBridgeRetryTest#eachRetryGetsTheRemainingBudgetAsDeadline` (60 s bütçe, saat +4 s sonra hata, jitter 0) | `calls.get(1).deadline() == 56s` (saat donuk, tam eşitlik); `calls.get(0).deadline() == 60s` |
| AC4 | `StreamBridgeRetryTest#nextBackoffBeyondTheBudgetStopsRetrying` (saat +59.95 s, initial=max=100ms, jitter=b) ve `#budgetSpentDuringBackoffStartsNoCall` (backoff bekleme sırasında saat +60 s) | 1 çağrı; `error` UNAVAILABLE son durum; `exhausted == 1` |
| AC6 | `StreamBridgeRetryTest#shutdownCancelDuringBackoffStartsNoCall` (initial=max=500ms, jitter=b, `CountingLease`) | Hatadan sonra `retryPending()` ve `pendingTimers() == 1`; `cancelForShutdown` sonrası `error` CANCELLED; `during(2.5s)` (5x backoff) boyunca `calls == 1`; `closes == 1`; `admittedCount == 0`; `pendingTimers()` 0 |
| AC6 | `#clientGoneDuringBackoffStartsNoCall` (`simulateClientGone()`), `#failedWriteOfAQueuedChunkDuringBackoffStopsRetry` (`failSends` true yapılır, sonra gate açılır) | Aynı; olay yok ya da yalnızca daha önce yazılanlar; `closes == 1`; `during(2.5s)` boyunca `calls == 1` |
| AC6 | `#shutdownBeginningStopsRetries` (drain 10 s; `CompletableFuture.runAsync(registry::stop)`; `!isAccepting` beklenir; sonra `onError(UNAVAILABLE)`) ve `#shutdownDuringBackoffEndsWithLastStatus` (backoff 500 ms) | `calls == 1`; tek `error` UNAVAILABLE; `stop` future `atMost(5s)` içinde biter (beklenen birkaç ms, drain 10 s); `exhausted == 0`; `pendingTimers()` 0 |
| AC8 | `#idleCannotFireDuringBackoff` (idle 200 ms, initial=max=3s, jitter=b; chunk0 alınmış) | Hatadan sonra `timerArmed() && retryPending()`, `pendingTimers() == 1`; `during(1s)` (5x idle, backoff'un 1/3'ü) boyunca `timeouts_total == 0`, `calls == 1`; `atMost(10s)` içinde 2. çağrı gelir; FIRST_CHUNK kurulu |
| AC8 | **Genişleyen** `StreamTimerTest#scheduleRetryOccupiesTheSingleSlotAndCloseCancelsIt`, `#scheduleRetryAfterCloseIsRefused`, `#scheduleRetryOnStoppedSchedulerIsRefused`, `#startWithZeroCreditArmsFirstChunkOnFirstGrant`, `#callEndedPreventsCreditFromArmingIdle`, `#retryTaskRunsOutsideTheTimerLock` | `pendingTimers` ve kurulu tür beklenen değerleri verir; false dönüşler; callback lock dışında çalışır |
| AC9 | `RetryAndResumeTest#retryMetricsArePreRegisteredAndExposed` ve yukarıdaki sayaç kontrolleri | Başlangıçta 0; iki endpoint 200 |
| AC10 | **Genişleyen** `ConnectionModeConfigTest#codeDefaultsApplyWithoutApplicationYaml` (3/100ms/2s), yeni `#retryIsBoundFromRelaxedNames` (`--lab.retry.max-attempts=5 --lab.retry.initialBackoff=50ms`), `@ParameterizedTest #invalidRetryFailsAtStartup` (`max-attempts=-1`, `=11`, `initial-backoff=0s`, `max-backoff=-1s`, `initial-backoff=3s`, `max-backoff=60s`) | Değerler eşleşir; yığında property adı geçer |
| AC11 | Değişmeyen tüm testler (özellikle `StreamEndToEndTest#unavailableMidStream`, `StreamBridgeTest`, `StreamBridgeTimeoutTest`, `GracefulShutdownTest`, `OverloadPolicyTest`, `TimeoutAndCancelTest`). Coordinator: `git diff --stat -- proto simulator`, `gateway/src` üzerinde `Thread.sleep` araması | Yeşil, boş, boş |
| AC12 | README okunur | Bölüm, tablolar ve hipotez işaretleri yerinde |

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- Tam doğrulama: `./mvnw -B verify`
- Odaklı: `./mvnw -B -pl gateway -am verify -Dtest="RetryPolicyTest,StreamBridgeRetryTest,StreamTimerTest,StreamBridgeTest,StreamBridgeTimeoutTest,UpstreamFlowControlTest,RetryAndResumeTest,StreamEndToEndTest,TimeoutAndCancelTest,GracefulShutdownTest,OverloadPolicyTest,ConnectionModeConfigTest" -Dsurefire.failIfNoSpecifiedTests=false`
- Kararlılık: `./mvnw -B -pl gateway -am test` en az 3 kez. Eski sınıf kalıntısı görülürse `./mvnw -B -pl gateway clean`.

**Riskler:**
- **R1 (kilit sırası):** `creditLock → timerLock` ve `sendLock → timerLock`. `creditLock` asla `sendLock` altında alınmaz, ters yön de yoktur. Reviewer, `handBackCredit`'in `sendLock` dışında kaldığını ve `registry.isAccepting()`'in hiçbir lock altında çağrılmadığını kontrol etmelidir.
- **R2 (Dekker deseni):** `current`, `call` ve `terminal` volatile olmalıdır. `startAttempt` ile claim arasındaki yarışta gereksiz bir çağrı başlayıp hemen iptal edilebilir; bu durumda simulator `started` +1, `cancelled` +1 görür. Kabul edilir.
- **R3 (geriye uyum):** Köprünün kendi observer metotları attempt 1 içindir. Bir test hem bunları hem `start()`'ı kullanmamalıdır.
- **R4 (e2e süre):** varsayılan backoff ile `unavailableMidStream` ~0,7 s uzar. Bu, 10 s'lik sınırın içindedir.
- **R5 (seq sürekliliği):** `nextSeq = seq + 1` güveni simulator'a dayanır. Atlamalı ya da tekrarlı bir upstream algılanmaz (README).
- **R6 (zamanlama):** e2e testleri tam jitter nedeniyle süre iddiası içermez. Shutdown/cancel-during-backoff testleri yalnızca unit düzeyindedir. Bilinçli kesinti: deterministik olmayan e2e testleri eklenmez.
- **R7:** grpc-java'nın şeffaf retry'ı açık kalır. Interceptor ve simulator hataları bundan etkilenmez (varsayım A5).

**Doğrulanmamış varsayımlar (implementer jar ve kaynaklara karşı kontrol etmeli):**
- **A1:** grpc 1.81.0'da `ClientCallImpl.request`, `start`'tan önce çağrılırsa "Not started" fırlatır. `ClientCalls`, `beforeStart`'ı `call.start`'tan önce, `generateTokens` thread'inde çağırır. Başlangıç kredisini `call.start`'tan sonra (`onStart` içinde) ister.
- **A2:** `disableAutoRequestWithInitial(0)` geçerlidir (`>= 0`). Kredi 0 iken in-process `isReady()` false olur. `UpstreamFlowControlTest`'in yeni testi bunu kanıtlar.
- **A3:** `beforeStart` içinde yapılan cancel, ardından `call.start`'ın fırlatmasıyla sonuçlanabilir (mevcut davranış); `startCall`'daki catch bunu yakalar.
- **A4:** Spring 7'de `ResponseBodyEmitter.onCompletion(Runnable)` final değildir ve override edilebilir.
- **A5:** grpc-java şeffaf retry'ı, interceptor'ün başarısız ettiği çağrıları tekrar etmez, çünkü interceptor transport'un üstündedir.
- **A6:** gRPC `onClose`'dan sonra listener'a mesaj teslim etmez. `cancel()`, listener callback'i içinden güvenle çağrılabilir.
- **A7:** JaCoCo için coverage eşiği yoktur (`pom.xml` kontrol edilmeli).

**Açık sorular** (engelleyici değil; varsayılanlar kararlaştırıldı, kullanıcı farklı isterse plan değişir):
1. Resume pozisyonu: isteğin metni "son yazılan + 1" diyor; plan "son alınan + 1" seçiyor (D2).
2. `max-attempts` toplam çağrıyı değil retry sayısını ifade ediyor.
3. Drain sırasında retry yapılmıyor.
4. `retries_total` `reason` değerleri `restart|resume`.
5. Varsayılanlar 100ms/2s; üst sınır 10.
