# Plan: gateway-shared-channel

## 1. Davranış ve kabul kriterleri

Gateway, `lab.connection-mode=shared` modunda lifecycle tarafından yönetilen tek bir `ManagedChannel` açar. `GET /stream` isteğini simulator'daki `TokenStreamService.GenerateTokens` çağrısına köprüler ve chunk'ları SSE olarak iletir. gRPC hatalarını SSE `error` olayına çevirir. Kanal READY değilse readiness probe'u DOWN döner. Graceful shutdown sırası şöyledir: yeni istekler reddedilir, aktif akışlar süre sınırı içinde boşaltılır, ardından kanal kapanır. `proto/` ve `simulator/` değişmez.

SSE sözleşmesi (karar):
- `event: chunk`, `id: <seq>`, `data: {"requestId":"…","seq":N,"text":"…","last":bool}`
- `event: complete`, `data: {"requestId":"…","chunks":N}`. `last=true` chunk'ından hemen sonra gönderilir ve emitter tamamlanır.
- `event: error`, `data: {"requestId":"…","code":"<grpc Status.Code adı>","message":"<status description veya boş>"}`. Ardından emitter normal şekilde `complete()` edilir. `completeWithError` kullanılmaz, çünkü yanıt commit edildikten sonra MVC hata işleme devreye girer.
- Her akış en fazla bir terminal olay alır (`complete` veya `error`) ve bu olaydan sonra başka olay gelmez.

Kabul kriterleri:
- **AC1 (başarılı akış):** `GET /stream?chunks=5&intervalMs=10` çağrısı 200 döner. `Content-Type` `text/event-stream`, `X-Request-Id` başlığı dolu (UUID). İstemci `id` 0..4 sırasıyla tam 5 `chunk` olayı alır, her `data` aynı requestId'yi taşır. Ardından tek bir `complete` gelir, `error` gelmez ve bağlantı kapanır.
- **AC2 (hata akışı):** `chunks=10&fault=UNAVAILABLE&faultAtSeq=3` çağrısında `chunk` id 0,1,2 gelir, ardından tek bir `error` olayı (`"code":"UNAVAILABLE"`, message `injected FAULT_TYPE_UNAVAILABLE at seq 3` içerir). `complete` gelmez, akış kapanır. `fault=INTERNAL&faultAtSeq=0` çağrısında yalnızca `error` (`INTERNAL`) gelir. `chunks=0` çağrısında simulator reddeder ve `error` (`INVALID_ARGUMENT`) gelir.
- **AC3 (parametre hatası):** `fault=FOO` veya `chunks=abc` upstream'e gitmeden HTTP 400 döner.
- **AC4 (readiness UP):** Simulator ayaktayken `/actuator/health/readiness` belirli bir süre içinde 200 döner. `upstreamChannel` bileşeni readiness grubunun içindedir.
- **AC5 (readiness DOWN):** Simulator durdurulunca `/actuator/health/readiness` belirli bir süre içinde 503 döner. `/actuator/health/liveness` 200 kalır, çünkü upstream liveness'a dahil değildir.
- **AC6 (shutdown, boşaltma):** Context kapanmaya başladığında devam eden bir akış (`chunks=20&intervalMs=50`, drain-timeout 5s) `complete` ile biter. `close()` döner ve kanal `isTerminated()==true` olur.
- **AC7 (shutdown, red ve zorla iptal):** Boşaltma sürerken yapılan yeni `GET /stream` isteği 503 alır (`Retry-After` başlığıyla, SSE açılmaz). Drain-timeout içinde bitmeyen (HANG) akış tek bir `error` olayı alır (`"code":"CANCELLED"`, `"message":"gateway shutting down"`) ve kapanır. `close()` en geç drain-timeout + 2s zorla iptal beklemesi + kanal kapanma süresi içinde döner. Kanal sonlanmış olur.
- **AC8 (mod konfigürasyonu):** Varsayılan değer `shared`'dir. `lab.connection-mode=per-request` ile uygulama başlangıçta bağlama hatasıyla açılmaz.
- **AC9:** Testlerdeki her SSE akışında tam olarak bir terminal olay (`complete` veya `error`) bulunur ve sonrasında olay gelmez.
- **AC10:** `git diff -- proto simulator` boştur ve `./mvnw -B verify` yeşildir.

## 2. İlgili dosyalar, mevcut yaklaşım ve önerilen değişiklik

### Doğrulanmış gerçekler (kodda okundu)
- `pom.xml`: modüller proto, simulator, gateway, loadgen. Java 26, Boot 4.1.1, grpc 1.81.0 (`grpc-bom` Boot BOM'undan önce import ediliyor). `dependencyManagement` yalnızca `proto` artefaktını yönetiyor, `simulator` yönetilmiyor.
- `gateway/pom.xml`: proto, grpc-netty-shaded, spring-boot-starter-webmvc ve spring-boot-starter-actuator var. Test bağımlılığı yok. `spring-boot-maven-plugin` repackage çalışıyor; test scope bağımlılıklar fat-jar'a girmez.
- `proto/pom.xml`: grpc-protobuf ve grpc-stub'ı compile scope'ta taşıyor, yani `TokenStreamServiceGrpc` stub'ları gateway'de hazır.
- `SimulatorServer.start(ServerBuilder<?>, SimulatorConfig)`, `port()`, `stop()` ve `close()` public. `stop()` graceful çalışıyor ve tekrar çağrılabilir. `SimulatorConfig(int port, Duration shutdownGrace, int schedulerThreads, RequestLimits limits)` ve `RequestLimits.DEFAULT` public. Simulator testleri aynı kalıbı kullanıyor: `InProcessServerBuilder.forName(InProcessServerBuilder.generateName())` + `InProcessChannelBuilder.forName(name)`.
- Simulator geçersiz istekleri `INVALID_ARGUMENT` ile reddediyor (`request_id` zorunlu, en fazla 128 karakter; `chunk_count` 1 ile 1_000_000 arasında; gecikmeler en fazla 60000 ms). Fault açıklaması `"injected " + type.name() + " at seq " + nextSeq`. HANG yalnızca cancel, deadline veya server shutdown ile biter.
- `application.yaml`: virtual threads açık, `server.shutdown: graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 20s`, `management.endpoint.health.probes.enabled: true`.
- CI (`.github/workflows/ci.yml`) `./mvnw -B verify` çalıştırıyor.
- Yerel `.m2` önbelleğinde 4.1.1 sürümleri var: `spring-boot-starter-test`, `spring-boot-health`, `spring-boot-web-server`, `spring-boot-webmvc-test`. Awaitility, junit-jupiter ve assertj simulator pom'unda sürümsüz kullanılıyor, yani BOM tarafından yönetiliyorlar.

### Önerilen tasarım (ana paket `io.github.cemharput.streamlab.gateway`)

**Konfigürasyon:** `config/GatewayProperties.java` bir `@ConfigurationProperties(prefix = "lab")` record'udur.
- `ConnectionMode connectionMode`: yalnızca `SHARED` değerini içeren enum, varsayılan `SHARED`.
- `Upstream upstream(String target = "localhost:50051", Duration streamDeadline = 60s)`
- `Shutdown shutdown(Duration drainTimeout = 10s, Duration channelTerminationTimeout = 5s)`

Compact constructor'larda pozitiflik kontrolü yapılır. Varsayılanlar `@DefaultValue` ile verilir. Kayıt `UpstreamConfiguration` üzerindeki `@EnableConfigurationProperties` ile yapılır.

**Lifecycle fazları:** `LifecyclePhases.java` final bir sabit sınıfıdır.
- `UPSTREAM_CHANNEL = SmartLifecycle.DEFAULT_PHASE - 4096`: web sunucusundan önce başlar, ondan sonra durur.
- `STREAM_ADMISSION = SmartLifecycle.DEFAULT_PHASE - 512`: web graceful-shutdown fazından önce durur. Böylece boşaltma sırasında Tomcat hâlâ bağlantı kabul eder ve yeni istekler 503 alır.

**Upstream paketi (`upstream/`):**
- `ChannelProvider.java` (public):
  ```java
  public interface ChannelProvider {
      Lease acquire();                                  // çalışmıyorsa IllegalStateException
      ConnectivityState state(boolean requestConnection);
      interface Lease extends AutoCloseable {
          io.grpc.Channel channel();
          @Override void close();                       // shared: no-op
      }
  }
  ```
  `Lease`, bir sonraki per-request modunun çağrı başına kanal kapatabilmesi için var. Bu feature'da tek implementasyon bulunuyor.
- `ManagedChannelFactory.java`: `@FunctionalInterface ManagedChannel create(String target)`. Bu, test için ayrılmış bağlantı noktasıdır.
- `SharedChannelProvider.java`: `ChannelProvider` ve `SmartLifecycle` implementasyonu, faz `UPSTREAM_CHANNEL`.
  - `start()`: `factory.create(target)` ile kanalı oluşturur, `getState(true)` ile bağlantıyı erken başlatır. `notifyWhenStateChanged` döngüsüyle durum geçişlerini loglar ve çalışırken IDLE'a düşünce `getState(true)` ile yeniden bağlanmayı ister. Bu yalnızca kanal düzeyinde bağlantı tutmaktır; akış retry/reconnect değildir.
  - `stop()`: `running=false`, `channel.shutdown()`, ardından `awaitTermination(channelTerminationTimeout)`. Süre dolarsa `shutdownNow()` ve 2s `awaitTermination` (`SimulatorServer.stop` ile aynı desen).
  - `isTerminated()` public'tir ve test için kullanılır.
- `UpstreamChannelHealthIndicator.java`: `HealthIndicator`. `provider.state(true) == READY` ise UP, değilse DOWN döner; `state` detail olarak eklenir.
- `UpstreamConfiguration.java` (`@Configuration(proxyBeanMethods=false)`) şu bean'leri tanımlar:
  - `nettyChannelFactory`: `target -> NettyChannelBuilder.forTarget(target).usePlaintext().build()`, sınıf `io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder`.
  - `SharedChannelProvider`
  - Bean adı `upstreamChannelHealthIndicator`. Boot bileşen adını "upstreamChannel" olarak türetir.

**Stream paketi (`stream/`):**
- `ActiveStreamRegistry.java` (public): `SmartLifecycle`, faz `STREAM_ADMISSION`.
  - `start()` sırasında `accepting=true` olur.
  - `tryRegister(StreamBridge)`, kabul kontrolü ve eklemeyi tek bir `ReentrantLock` altında yapar; red durumunda false döner. `unregister` aynı lock altında `Condition.signalAll` çağırır.
  - `stop()` sırası: (1) `accepting=false`; (2) `active` boşalana kadar en fazla `drainTimeout` bekler; (3) kalan her bridge için `cancelForShutdown()` çağırır; (4) en fazla `FORCED_CANCEL_WAIT=2s` daha bekler ve boşalmazsa loglar.
  - `isAccepting()` ve `activeCount()` public'tir.
- `StreamBridge.java` (package-private): `ClientResponseObserver<GenerateRequest, TokenChunk>` implementasyonu. Tek terminal sonucun sahibidir.
  - Durum tek bir `lock` altında tutulur: `terminal` alanı (`StreamOutcome`: `COMPLETED`, `UPSTREAM_ERROR`, `CLIENT_GONE`, `SHUTDOWN_CANCELLED`) ve `ClientCallStreamObserver requestStream`.
  - `terminate(outcome, action)` yalnızca ilk çağrıda çalışır. Emitter'a bir terminal olay gönderir veya hiç göndermez, `complete()` çağırır, lease'i kapatır, registry'den çıkar ve loglar. Sonraki çağrılar no-op'tur.
  - Tetikleyiciler:
    - `onNext` → `chunk` gönderilir; `last` true ise `complete` olayı ve `COMPLETED`.
    - `onError` → `Status.fromThrowable` ile `error` olayı ve `UPSTREAM_ERROR`.
    - `onCompleted` → `last` görülmediyse `error` (`code:"INTERNAL"`, `message:"upstream ended without last chunk"`); görüldüyse no-op.
    - `cancelForShutdown()` → `error` (`CANCELLED`, "gateway shutting down") gönderilir, ardından `requestStream.cancel(...)`.
    - `emitter.send` IOException fırlatırsa, ya da `emitter.onError`/`onTimeout`/`onCompletion` terminalden önce gelirse → `requestStream.cancel(...)` ve `CLIENT_GONE`. Bu durumda olay gönderilmez.
- `StreamParameters.java`: record. Spring MVC `@RequestParam` bağlaması kullanılır.

  | HTTP parametresi | Varsayılan | GenerateRequest alanı |
  |---|---|---|
  | `chunks` (int) | 10 | `profile.chunk_count` |
  | `firstDelayMs` (int) | 0 | `profile.first_chunk_delay_ms` |
  | `intervalMs` (int) | 50 | `profile.chunk_interval_ms` |
  | `fault` (`FaultParam {NONE, UNAVAILABLE, INTERNAL, HANG}`) | NONE | `fault.type` (`FAULT_TYPE_*`) |
  | `faultAtSeq` (int) | 0 | `fault.at_seq` |
  | `seed` (long) | 0 | `seed` |

  `request_id` gateway tarafında `UUID.randomUUID().toString()` ile üretilir (36 karakter). `chunk_size_bytes` ve `resume_from_seq` set edilmez. Değer doğrulaması simulator'a bırakılır (`INVALID_ARGUMENT` → SSE `error`).
- `SseEvents.java`: `ChunkEvent`, `CompleteEvent` ve `ErrorEvent` record'ları. JSON gönderimi `SseEmitter.event().name(..).id(..).data(obj, MediaType.APPLICATION_JSON)` ile yapılır.
- `StreamController.java`: `@GetMapping(path="/stream", produces=TEXT_EVENT_STREAM_VALUE)` ve dönüş tipi `ResponseEntity<SseEmitter>`.
  - Registry reddederse `ResponseEntity.status(503).header("Retry-After","1").build()` döner.
  - Kabul ederse:
    1. `new SseEmitter(streamDeadline.toMillis() + 5000)` oluşturulur. Pay gRPC deadline'ının emitter timeout'undan önce dolmasını sağlar.
    2. Bridge oluşturulur, `tryRegister` çağrılır ve `provider.acquire()` ile lease alınır.
    3. `TokenStreamServiceGrpc.newStub(lease.channel()).withDeadlineAfter(streamDeadline)` ile `generateTokens(request, bridge)` çağrılır.
    4. Header `X-Request-Id` ile birlikte `ResponseEntity.ok()` döner.

**Kararlar:**
- **İstemci bağlantı kopmasında minimal temizlik:** Kopmada upstream çağrısı iptal edilir ve registry'den çıkılır. Bu, kapsam dışı olsa da doğruluk için gerekli. Yapılmazsa boşaltma asılı kalır ve aktif sayaç sızar. İptal zincirinin (simulator `active_generations`) doğrulanması bu feature'da test edilmez.
- **Backpressure:** Async stub'ın auto-request davranışı ve `onNext` içinde bloklayan `emitter.send` sayesinde sınırlı akış kontrolü kendiliğinden oluşur. Bounded queue veya semaphore eklenmez. gRPC callback executor'ı varsayılan bırakılır.
- **Readiness grubu:** `management.endpoint.health.group.readiness.include: readinessState,upstreamChannel` eklenir. Liveness'a dokunulmaz.

## 3. Uygulama adımları

1. `gateway/pom.xml` dosyasına test scope bağımlılıklar eklenir:
   - `spring-boot-starter-test`
   - `io.github.cemharput.streamlab:simulator:${project.version}` (parent'ta yönetilmediği için sürüm açıkça yazılır)
   - `io.grpc:grpc-inprocess`
   - `org.awaitility:awaitility`

   `spring-boot-starter-webmvc-test` eklenmez; MockMvc kullanılmayacak. Reactor sırası test bağımlılığı sayesinde simulator → gateway olarak kalır.
2. `gateway/src/main/resources/application.yaml` dosyasına `lab.connection-mode: shared`, `lab.upstream.target: localhost:50051`, `lab.upstream.stream-deadline: 60s`, `lab.shutdown.drain-timeout: 10s` ve `lab.shutdown.channel-termination-timeout: 5s` eklenir; yukarıdaki readiness grubu da eklenir. Toplam bütçe şöyle: registry fazı yaklaşık 10+2s, kanal fazı yaklaşık 5+2s; her biri ayrı ayrı 20s'lik faz sınırının altında.
3. `gateway/src/main/java/io/github/cemharput/streamlab/gateway/` altında main dosyaları oluşturulur: `LifecyclePhases.java`, `config/GatewayProperties.java`, `upstream/{ChannelProvider, ManagedChannelFactory, SharedChannelProvider, UpstreamChannelHealthIndicator, UpstreamConfiguration}.java`, `stream/{ActiveStreamRegistry, StreamBridge, StreamParameters, FaultParam, SseEvents, StreamController}.java`. `GatewayApplication.java` değişmez.
4. Test desteği gateway'in component-scan kökü dışında, `gateway/src/test/java/io/github/cemharput/streamlab/testsupport/` altında oluşturulur. Böylece manuel `SpringApplicationBuilder` ile kurulan context bu sınıfları istemeden taramaz.
   - `InProcessSimulatorConfig.java` (`@Configuration(proxyBeanMethods=false)`) iki bean tanımlar:
     - `@Bean(destroyMethod="stop") SimulatorServer`: `InProcessServerBuilder.forName(name)` ve `new SimulatorConfig(0, Duration.ofSeconds(2), 2, RequestLimits.DEFAULT)` ile başlatılır.
     - `@Bean @Primary ManagedChannelFactory`: `SimulatorServer` parametresi alır, böylece ondan sonra yok edilir. Hedefi yok sayıp `InProcessChannelBuilder.forName(name).build()` döner.
   - `SseClient.java`: `java.net.http.HttpClient` (HTTP/1.1) ile `sendAsync(..., BodyHandlers.ofLines())` yapar. Satırları `SseEvent(name, id, data)` kayıtlarına ayrıştırıp `CopyOnWriteArrayList`'e toplar. Akış sonu için bir `CompletableFuture` ve HTTP status/header'ları sunar.
5. `gateway/src/test/java/io/github/cemharput/streamlab/gateway/` altındaki testler:
   - `StreamEndToEndTest`: `@SpringBootTest(webEnvironment=RANDOM_PORT)`, `@Import(InProcessSimulatorConfig.class)`, port `@Value("${local.server.port}")` ile alınır. AC1, AC2, AC3, AC4 ve AC9'u kapsar.
   - `ReadinessDownTest`: aynı kurulum artı `@DirtiesContext`. `SimulatorServer` bean'i alınır, readiness'in 200 olması beklenir, `stop()` çağrılır, sonra 503 beklenir; liveness 200 kalır (AC5).
   - `GracefulShutdownTest`: context `new SpringApplicationBuilder(GatewayApplication.class, InProcessSimulatorConfig.class).properties("server.port=0", "lab.shutdown.drain-timeout=…").run()` ile manuel kurulur. Port `context.getEnvironment().getProperty("local.server.port", Integer.class)` ile okunur. AC6 ve AC7'yi kapsar; `close()` `CompletableFuture.runAsync` ile tetiklenir.
   - `ConnectionModeConfigTest`: aynı builder ile `lab.connection-mode=per-request` verilir, `assertThatThrownBy(run)` beklenir (AC8).
6. `README.md` gateway bölümüne kısa ekleme yapılır: `GET /stream` parametreleri, SSE olay adları ve `lab.*` ayarları. Modül tablosundaki durum güncellenir.

## 4. Doğrulama

| AC | Kontrol | Beklenen |
|---|---|---|
| AC1 | `StreamEndToEndTest#successStream`: `await().atMost(5s)` ile akış bitişi beklenir | Status 200, `X-Request-Id` dolu, `chunk` id 0..4 sırasıyla, tek `complete` |
| AC2 | `#unavailableMidStream`, `#internalAtFirstChunk`, `#invalidChunkCount` | id 0,1,2 + `error` UNAVAILABLE; yalnızca `error` INTERNAL; `error` INVALID_ARGUMENT; hiçbirinde `complete` yok |
| AC3 | `#badParameterRejected` (`fault=FOO`) | HTTP 400 |
| AC4 | `#readinessUp`: Awaitility ile `/actuator/health/readiness` | 200 |
| AC5 | `ReadinessDownTest` | Simulator stop sonrası 10s içinde 503, liveness 200 |
| AC6 | `GracefulShutdownTest#activeStreamDrains` (drain 5s, chunks 20 × 50ms): ilk chunk beklenir, sonra `close` | Akış `complete` ile biter, close future 10s içinde tamamlanır, `SharedChannelProvider.isTerminated()` true |
| AC7 | `#newRequestsRejectedAndHangingStreamCancelled` (drain 3s, `fault=HANG&faultAtSeq=2`): 2 chunk beklenir, `close` async başlatılır, `registry.isAccepting()==false` beklenir, yeni GET yapılır | Yeni GET 503 ve `Retry-After`; HANG akışında tek `error` CANCELLED "gateway shutting down"; close yaklaşık 3s+2s+5s içinde (test sınırı 15s) biter; kanal sonlanmış |
| AC8 | `ConnectionModeConfigTest` | `per-request` ile başlangıç hatası; varsayılanla context açılır |
| AC9 | Tüm akış testlerinde yardımcı assert | Terminal olay sayısı 1, terminalden sonra olay yok |
| AC10 | Coordinator `git diff --stat -- proto simulator` çalıştırır ve tam build alınır | Diff boş, exit 0 |

Testlerde `Thread.sleep` kullanılmaz; tüm beklemeler Awaitility `await().atMost(...)` ile yapılır.

**Komutlar** (repo kökünden; Windows'ta `.\mvnw.cmd`):
- Tam doğrulama: `./mvnw -B verify`
- Odaklı: `./mvnw -B -pl gateway -am verify`
- Tek sınıf: `./mvnw -B -pl gateway -am verify -Dtest=StreamEndToEndTest -Dsurefire.failIfNoSpecifiedTests=false`

**Riskler:**
- Boot 4 paket konumları depodan doğrulanamadı; implementer bunları çözümlenen jar'lara karşı kontrol etmeli. Varsayımlar:
  - `HealthIndicator`, `Health`, `Status` için `org.springframework.boot.health.contributor`. `spring-boot-health` jar'ının varlığı bunu destekliyor.
  - `@SpringBootTest` için `org.springframework.boot.test.context`.
  - `SpringApplicationBuilder` için `org.springframework.boot.builder`.
  - `@ConfigurationProperties` ve `bind.DefaultValue` için `org.springframework.boot.context.properties`.
  - `SseEmitter` için `org.springframework.web.servlet.mvc.method.annotation`.
  - `@LocalServerPort` ve `TestRestTemplate`/MockMvc konumlarındaki belirsizlik nedeniyle bilerek `local.server.port` property'si ve JDK `HttpClient` kullanıldı.
- Web sunucusu graceful-shutdown ve start/stop fazlarının Boot 4'te `DEFAULT_PHASE-1024` ve `-2048` olduğu varsayıldı. AC7'deki 503 testi sıralamayı uçtan uca kanıtlar; bu test kırmızı olursa `STREAM_ADMISSION` fazı yükseltilmeli.
- SSE `data` JSON'u Boot 4 varsayılan Jackson sürümüyle serileştirilir. Testler Jackson paketine bağlı kalmamak için `data` alanında alt dizi kontrolü yapar.
- Java 26 üzerinde Mockito/ByteBuddy agent uyarıları çıkabilir; mock kullanılmıyor.
- Yavaş SSE istemcisi gRPC'nin varsayılan executor thread'ini bloklar. Bu bilinçli olarak sonraki bounded concurrency feature'ına bırakıldı.
- Tomcat graceful shutdown'ın açık async istekleri beklediği varsayılıyor. Registry önce boşalttığı için bu durumun pratik etkisi yok.

**Varsayımlar:** Gateway SSE çıktısında chunk'ları JSON taşır; olay adları `chunk`, `complete` ve `error`'dur. HTTP parametre adları yukarıdaki tablodaki gibidir. Üretimde upstream hedefi plaintext `localhost:50051`'dir; TLS yoktur.

**Açık sorular:** Engelleyici soru yok. İstenirse olay ve parametre adları implementasyondan önce değiştirilebilir.
