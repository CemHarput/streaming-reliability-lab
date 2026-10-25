# Plan: benchmark-protocol-run

Doğrulanmış olgular (HEAD 1fa78dc; dosyalar doğrudan okundu, code-graph MCP kullanılmadı):

- **Build:** Maven 3.9.16 (wrapper). Root `pom.xml` modül sırası `proto, simulator, gateway, loadgen`. Enforcer yok. `pluginManagement` içinde compiler 3.14.1, surefire 3.5.4, `maven-jar-plugin` 3.4.2, `maven-dependency-plugin` 3.8.1, protobuf ve `spring-boot-maven-plugin` (4.1.1) var. JaCoCo 0.8.15 yalnızca `prepare-agent` ve `report` çalıştırıyor, eşik yok. CI ubuntu üzerinde temurin 26 ile `./mvnw -B verify` koşuyor, timeout 15 dk.
- **Simulator:** `SimulatorMain` ayarları yalnızca env'den okuyor: `SIMULATOR_PORT` (50051), `SIMULATOR_SHUTDOWN_GRACE_SECONDS` (10), `SIMULATOR_SCHEDULER_THREADS` (varsayılanı availableProcessors). `RequestLimits.DEFAULT` ayarlanamıyor. Graceful stop yalnızca JVM shutdown hook'unda çalışıyor. `simulator/pom.xml` yalnızca `exec-maven-plugin` (3.6.3) tanımlıyor. **Çalıştırılabilir artefakt yok:** jar'da classpath manifest'i yok, `lib/` kopyası yok. İstatistikler yalnızca gRPC `SimulatorAdminService.GetStats` ile okunabiliyor (`active_generations, started_total, completed_total, cancelled_total, faulted_total, rejected_total, chunks_emitted_total, flow_control_pauses_total`). HTTP yüzeyi yok. gRPC health ve reflection servisleri kayıtlı.
- **Gateway:** `spring-boot-maven-plugin repackage` ile `gateway/target/gateway-0.1.0-SNAPSHOT.jar` üretiliyor (yanında `.jar.original` da kalıyor). Başlatma `java -jar` ile. `application.yaml`:
  - `server.port` tanımlı değil (8080), `server.shutdown: graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 20s`, `spring.threads.virtual.enabled: true`.
  - `lab.*` değerleri README tablosuyla aynı: `connection-mode shared`, `upstream.target localhost:50051`, `stream-deadline 60s`, `limits` 100/reject/5s/100/16, `timeouts` 30s/30s, `retry` 3/100ms/2s, `shutdown` 10s/5s.
  - Exposure `health,metrics,prometheus`. **`shutdown` endpoint'i açık değil.** Readiness grubu `readinessState,upstreamChannel`.
  - Kanal `NettyChannelBuilder.forTarget(target).usePlaintext()` ile kuruluyor. Gateway `chunk_size_bytes` hiç set etmiyor (0, yani tek kelime).
- **Gateway metrikleri:**
  - `StreamMetrics`: `active_streams`, `queue_depth{queue=chunks|admission}`, `rejected_total{reason=limit|queue_timeout|shutdown|upstream_unavailable|interrupted}`, `timeouts_total{kind=first_chunk|idle}`, `retries_total{reason=restart|resume}`, `retry_exhausted_total`. Hepsi açılışta kaydediliyor.
  - `ConnectionModeMetrics`: `connection_mode{mode=shared|per_request}` = 1.
- **Loadgen:** `loadgen/target/loadgen-0.1.0-SNAPSHOT.jar` ve `target/lib/` (manifest `Class-Path: lib/...`).
  - Exit kodları 0 (run tamamlandı, outcome'lardan bağımsız), 1 (I/O ya da beklenmeyen hata), 2 (kullanım hatası).
  - `--output` / `--summary-output` dosyası varsa hata verir. Üst dizin mevcut olmalı.
  - Summary düz bir JSON: `type=summary, schemaVersion, label, target, concurrency, elapsedNanos, stopReason, requestsStarted, recordsWritten, outcome_<OUTCOME>...` ve percentil alanları.
  - JSONL: ilk satır `type=header`, son satır `type=footer` (`stopReason`, `requestsStarted`, `recordsWritten`, `interrupted` ...). I/O hatasında footer yok.
  - Outcome'lar: `COMPLETED, ERROR_EVENT, REJECTED, HTTP_ERROR, PROTOCOL_ERROR, NO_TERMINAL, RESET, TIMEOUT, CONNECT_FAILURE, INTERRUPTED, CLIENT_ERROR`.
- **Repo durumu:** `docs/benchmark*`, `bench/` ve `results/` yok. ADR'ler 0001..0004. `.gitignore` içinde `results/raw/` var, `bench/results` yok. Test konvansiyonu: `@Timeout(value=…, threadMode=SEPARATE_THREAD)`, `Thread.sleep` kullanılmıyor, `@TempDir`.

## 1. Davranış ve kabul kriterleri

Yeni bir Maven modülü `bench` eklenir. İçinde düz Java bir runner (`BenchMain`) bulunur. Runner yazılı protokolü (`docs/benchmark-protocol.md`, protocol version 1) uygular ve iki işi yapar:
- Her ölçüm hücresi (mode × concurrency × repetition) için simulator ile gateway'i ayrı, taze JVM'ler olarak başlatır, hazır olmalarını sınırlı sürelerle bekler, connection mode'u metrikten doğrular, warm-up ve ölçüm koşularını loadgen ile çalıştırır, öncesinde ve sonrasında metrik snapshot'ı alır, process'leri durdurur ve portların boşaldığını doğrular.
- Ham çıktıyı `bench/results/<run-id>/` altına, sabit şemalı bir `manifest.json` ile birlikte yazar.

Runner analiz yapmaz ve sıralama vermez. Stdout ve dokümanlarda performans iddiası yer almaz. Bayraklar yalnızca kayıttır, hiçbir veri silinmez.

### Runner kararı: yeni modül `bench` (Java 26, JDK API'leri)

Gerekçe ve alternatifler (ADR-0005'e girer):
- **Bash (Git Bash):** MSYS PID'leri Windows PID'leri değil. Süreç ağacını öldürmek için `taskkill /T /F` ayrımı gerekir. mintty altında `trap INT` native Java process'leri için güvenilir değil. JSON için `jq`, test için `bats` gerekirdi; ikisi de yeni bağımlılık. CI'da birim testi yapılamaz.
- **PowerShell:** yalnızca Windows'ta çalışır (ya da CI'a pwsh gerekir), test için Pester gerekir, Linux için ikinci bir implementasyon gerekir.
- **Python:** toolchain'de yok, yeni bir runtime eklenir.
- **Java modülü:** `ProcessBuilder`/`ProcessHandle` (descendants, pid, onExit), `HttpClient` (actuator) ve projedeki `proto` + `grpc-netty-shaded` (GetStats) ile tek ve taşınabilir bir implementasyon olur. `./mvnw verify` içinde birim testi yapılır, yeni üçüncü parti artefakt gerekmez.
  - Dezavantajları: script'ten daha fazla kod. Runner JVM'i de aynı makinede durur (ölçüm sırasında boşta bekler). Windows'ta `Process.destroy()` ile `destroyForcibly()` ikisi de `TerminateProcess` olduğu için gateway ve simulator'ın graceful shutdown'u tetiklenemez (bulgu F3).
- **Process kontrolü:** çocuklar Maven wrapper üzerinden değil, doğrudan `<java.home>/bin/java(.exe)` ile başlatılır. Böylece ara süreç ağacı oluşmaz. Durdurma sırası şöyle:
  1. Descendants snapshot'ı alınır.
  2. `destroy()` çağrılır (POSIX'te SIGTERM, hook'lar çalışır).
  3. Sınırlı süre `waitFor` beklenir.
  4. `destroyForcibly()` çağrılır.
  5. Snapshot'taki ve güncel descendants'ın hepsine `destroyForcibly()` uygulanır.
  6. `waitFor` beklenir.

  Hepsinin süresi sınırlıdır.
- **Ctrl+C:** runner'ın shutdown hook'u kayıtlı bütün çocukları öldürür, manifest'i `interrupted` olarak yazar ve yeni process başlatılmasını kilitle engeller. Çocukların Windows konsolundan CTRL_C alıp almadığı doğrulanmadı; hook her iki durumda da temizlik yapar. Runner'ın kendisi `TerminateProcess` ile öldürülürse (ör. pencere kapatma, mintty) hook çalışmaz. Bu boşluk iki önlemle kapatılır: her process başlar başlamaz PID'i manifest'e yazılır, her run'dan önce port dinleyici kontrolü yapılır (yetim process tespiti ve ipucu mesajı).
- **Artefaktlar:** runner Maven çağırmaz. Build ayrı ve belgelenmiş bir adımdır: `./mvnw -B package -DskipTests`. Böylece `mvnw`/`mvnw.cmd` ayrımı, benchmark sırasında build yapılması ve Maven JVM gürültüsü önlenir.
  - Gateway jar'ı ve loadgen jar'ı + `lib/` doğrudan kullanılır.
  - Simulator için `bench` modülü `simulator`'a `runtime` scope ile bağımlıdır. `copy-dependencies` `bench/target/lib`'e simulator jar'ını ve runtime kapanışını (proto, grpc-*) kopyalar. Simulator `java -cp <bench/target/lib içindeki jar'lar, ada göre sıralı, açık liste> io.github.cemharput.streamlab.simulator.SimulatorMain` ile başlatılır. Açık ve sıralı liste wildcard'ın belirsiz sırasını önler ve hash'lenebilir.
  - Manifest her artefakt için şunları kaydeder: path, size, sha256, lastModified. Ayrıca git commit, dirty flag ve HEAD commit zamanı kaydedilir. Jar HEAD commit zamanından eskiyse `ARTIFACT_OLDER_THAN_HEAD` bayrağı konur (sezgisel bir kontrol).

### Kabul kriterleri

- **AC1 (build/sınırlar):**
  - Root `pom.xml`'e `<module>bench</module>` eklenir.
  - `./mvnw -B verify` art arda 3 kez yeşil geçer.
  - `git diff --stat -- gateway simulator proto loadgen` boş kalır.
  - `bench` runtime bağımlılıkları yalnızca `proto`, `grpc-netty-shaded` ve `simulator` (ve onların reactor'da zaten bulunan kapanışı). Yeni üçüncü parti artefakt yoktur (`dependency:tree` ile kontrol edilir).
  - Testler gerçek gateway, simulator veya loadgen process'i başlatmaz.
- **AC2 (CLI):**
  - Bölüm 2'deki seçenekler hem `--name value` hem `--name=value` biçiminde kabul edilir.
  - Geçersiz, bilinmeyen, tekrarlanan ya da eksik değerli argümanlar, `ramp-up ≥ duration/warmup`, tekrarlı concurrency, eksik veya tekrarlı mode-order, eşit portlar, aralık dışı değerler: tek satır mesaj + `Try --help.` ile **exit 2** verir; hiçbir şey başlatılmaz, dizin oluşturulmaz.
  - Ön koşul hataları da exit 2 verir: artefakt eksik (dry-run dışında), port dinleniyor, `--checklist ask` ile terminal yok.
  - `--help` exit 0 döner.
  - Bütün planlanan run'lar koşulduysa exit 0 döner; bayraklar exit kodunu etkilemez.
  - Dizin oluştuktan sonra harness ya da run hatası olursa manifest `failed` olur ve exit 1 döner.
  - Kesintide manifest `interrupted` olur. Exit kodunu JVM belirler; test edilmez, belgelenir.
- **AC3 (plan):**
  - Concurrency dış döngüdür. Her concurrency için repetition `r` (0 tabanlı): `r` çiftse `[A,B]`, tekse `[B,A]`. A ve B, `--mode-order`'daki birinci ve ikinci mode'dur. N=4 için sıra `ABBA ABBA` olur.
  - Run id'leri tekil ve sıralıdır.
  - Kullanılan sıra manifest'te `parameters.plan.alternation` ve her run'ın `orderLabel` alanında kaydedilir.
- **AC4 (dry-run):** `--dry-run` şunları yazdırır: preset, çözülmüş parametreler, pinlenmiş ayarlar, artefakt durumu (eksikse `MISSING` uyarısı ve yine exit 0), oluşacak run-id ve dizinler, sıralı run listesi, her run için simulator/gateway/warm-up/measured loadgen'in tam komut satırları ve env override'ları, nominal toplam süre. Hiçbir process başlatılmaz (yalnızca salt-okur `git` komutları çalışır), hiçbir dosya ya da dizin oluşturulmaz.
- **AC5 (layout/run-id):**
  - Run-id `yyyyMMdd'T'HHmmss'Z'-<shortCommit|nogit>[-dirty][-smoke]` biçimindedir.
  - Dizin `Files.createDirectory` ile atomik oluşturulur. Varsa `-2`…`-99` eki denenir, sonra hata verilir. Var olan dizin ya da dosyanın üzerine asla yazılmaz.
  - Her run'ın alt dizini Bölüm 2'deki sabit dosya adlarını içerir.
- **AC6 (manifest):**
  - `manifest.json` `schemaVersion=1` ve `protocolVersion=1` taşır, Bölüm 2'deki anahtarları içerir.
  - Her durum değişikliğinden sonra (plan, process start/PID, run sonu, final, interrupt) atomik yazılır (tmp dosyaya yazılır, sonra `ATOMIC_MOVE`).
  - Run'ın `status` alanı `planned|running|completed|failed|interrupted|not_run` değerlerinden birini alır.
- **AC7 (hazırlık):**
  - Simulator hazır sayılması için `GetStats` başarılı olmalı (≤30 s).
  - Gateway hazır sayılması için `/actuator/health/readiness` 200 + `UP` dönmeli (≤90 s). Ardından `/actuator/metrics/connection_mode` içindeki `mode` tag değerleri tam olarak beklenen tag olmalı (`shared` ya da `per_request`) ve VALUE 1.0 olmalı. Uyuşmazlık `CONNECTION_MODE_MISMATCH` ile run hatasıdır.
  - Bekleme sırasında process ölürse beklemeden hemen hata verilir. Bütün beklemeler sınırlıdır.
- **AC8 (snapshot ve bayraklar):**
  - `ready`, `pre` ve `post` snapshot'ları gateway metriklerini (ham gövde ve çıkarılmış değer) ve simulator stats'ı içerir.
  - Bayraklar Bölüm 2'deki sözlükten gelir. Bayraklı run'lar ve dosyaları yerinde kalır.
- **AC9 (ortam ve checklist):**
  - Ortam bilgisi: OS, CPU modeli (kaynağı ile birlikte), logical processor sayısı, physical core sayısı (best effort), RAM, Java bilgisi, git commit/dirty/commit zamanı, Windows güç planı, TIME_WAIT sayısı (best effort), ortamdan çıkarılan env değişken adları.
  - Her ölçümden önce kaba bir CPU-load örneği alınır.
  - Checklist `ask|yes|skip` modlarında çalışır; cevaplar kaynağıyla kaydedilir. `skip` ya da `no` cevabı veri gizlemez, yalnızca bayrak koyar.
- **AC10 (temizlik):** başarı, run hatası (ör. hazırlık timeout'u) ve kesinti (hook) sonrasında:
  - Kayıtlı bütün çocuk process'ler ve descendants'ları ölüdür.
  - Gateway ve simulator portlarında dinleyici kalmaz (≤30 s içinde doğrulanır).
  - Process log'ları stream başına 16 MiB ile sınırlıdır; kesilen byte sayısı manifest'e yazılır.
- **AC11 (manuel smoke):**
  - `--preset smoke` gerçek simulator ve gateway'e karşı exit 0 ile biter.
  - Her iki mode için completed run'lar, geçerli bir manifest ve run başına dosyalar oluşur.
  - İkinci bir smoke yeni bir dizin oluşturur; ilki değişmez.
  - Implementer yalnızca dizin listesini ve manifest şeklini raporlar, sayı raporlamaz.
- **AC12 (dokümanlar):**
  - `docs/benchmark-protocol.md` şunları içerir: soru, cevaplayamadıkları, değişkenler ve pinlenmiş ayarlar, prosedür, ortam kaydı, checklist, stabilite bayrakları, layout ve şema, arşivleme, harness sınırları.
  - README'ye şunlar eklenir: modül satırı, durum paragrafı, komutlarla birlikte bir "Benchmark protocol" bölümü ve açık bir "no performance claim" cümlesi.
  - `docs/adr/0005-benchmark-runner.md` eklenir.
  - `.gitignore`'a `bench/results/` eklenir.
  - Dokümanlarda ölçüm sayısı yoktur (pinlenmiş ayar değerleri hariç).
- **AC13 (hijyen):**
  - Testlerde `Thread.sleep` yok; her test sınıfında `@Timeout(threadMode=SEPARATE_THREAD)` var.
  - Beklemeler Awaitility ya da latch ile sınırlıdır ve en az 5 kat pay taşır.
  - Sunucu ve process'ler `finally` ya da `@AfterEach` içinde kapatılır.
  - `bench` testlerinin toplamı yaklaşık 30 s altında kalır.
- **AC14 (defekt):** smoke ya da testler gateway, simulator veya loadgen'de bir defekt gösterirse iş durur ve kanıtıyla raporlanır. Feature `blocked` olur. O modüller değiştirilmez, assert zayıflatılmaz.

## 2. Dosyalar, semboller ve tasarım

### Pom

- **Root `pom.xml`:** `<module>bench</module>` eklenir (loadgen'den sonra). Yeni property ya da plugin sürümü eklenmez.
- **`bench/pom.xml`:**
  - Bağımlılıklar: `proto` (compile, yönetilen), `io.grpc:grpc-netty-shaded` (compile, BOM'dan), `simulator` `${project.version}` `runtime`. Test kapsamında `junit-jupiter`, `assertj-core`, `awaitility` (sürümsüz).
  - `maven-jar-plugin`: `mainClass=io.github.cemharput.streamlab.bench.BenchMain`, `addClasspath`, `classpathPrefix=lib/`, `addDefaultImplementationEntries`.
  - `maven-dependency-plugin` `copy-dependencies`: `package` fazında, `includeScope=runtime`, `${project.build.directory}/lib`.
  - Bu lib dizini hem runner'ın hem simulator'ın classpath'idir. AC1 kapsamında `dependency:tree` ile içeriğinin yalnızca simulator kapanışı olduğu doğrulanır.

### Paket `io.github.cemharput.streamlab.bench` (main)

- `BenchMain`:
  - `main` → `System.exit(run(args, out, err, in))`.
  - `run` içinde `System.exit` yoktur.
  - Hook'u kurar: `shutdownHook(BenchRunner)` (test edilebilir statik fabrika).
- `BenchOptions` (record), `BenchOptionsParser` (loadgen'in parser stiliyle), `UsageException`, `Preset` (`DEFAULT`, `SMOKE`), `ConnectionMode` (`SHARED("shared","shared")`, `PER_REQUEST("per-request","per_request")`: property değeri ve metrik tag'i).
- `PlanBuilder` → `RunPlan(List<PlannedRun>)`. `PlannedRun(int index, String id, int concurrency, ConnectionMode mode, int repetition, String orderLabel)`. `id` = dizin adı, ör. `007-c100-per-request-r2` (index 3 haneli ve 1 tabanlı; repetition 1 tabanlı).
- `RunId` (format ve `createUniqueDirectory(Path outputDir, Instant, git, smoke)`), `ResultLayout` (bütün path ve dosya adları).
- `Artifacts` (auto-detect ve override'lar, sha256 için `MessageDigest`, `ArtifactInfo`).
- `ProcessSpecs`: `ProcessSpec(name, List<String> command, Map<String,String> envOverrides, Set<String> envRemoved, Path workDir, Path logFile)` üretir.
  - **Env sanitizasyonu:** `JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, `_JAVA_OPTIONS`, `SPRING_*`, `LAB_*`, `MANAGEMENT_*`, `SERVER_*`, `SIMULATOR_*` çıkarılır. Çıkarılanların yalnızca adları kaydedilir.
- `ChildProcess` (arayüz: `pid`, `isAlive`, `destroy`, `destroyForcibly`, `waitFor(Duration)`, `descendants`, `exitValue`), `JdkChildProcess`, `ProcessLauncher` (arayüz).
- `ProcessSupervisor`:
  - `start(spec)` synchronized çalışır; `closed` olduktan sonra `IllegalStateException` fırlatır.
  - `stop(child, gracefulWait, forcedWait)` → `StopResult(method, exitCode, durationMs)`.
  - `killAll(Duration bound)`.
- `BoundedLogPump`: stdout ve stderr birleşik (`redirectErrorStream(true)`). Platform daemon thread'idir. İlk `maxBytes` (16 MiB) yazılır, sonrası okunup atılır (çocuk bloklanmasın diye). Kapanışta bir kesme notu ekler; `bytesWritten` ve `bytesDropped` sayar.
- `PortProbe`: `listenerPresent(port)` için `127.0.0.1:port`'a connect denenir (timeout 3 s). Bind kontrolü kullanılmaz: Windows'ta `SO_REUSEADDR`/TIME_WAIT semantiği yanlış "dolu" ya da "boş" sonucu verebilir. `awaitNoListener(ports, timeout, interval)` sağlanır.
- `Ticker` (arayüz: `nanoTime`, `sleep`): production `Thread.sleep` kullanır, testler sahte ticker kullanır.
- `Readiness.await(probe, timeout, interval, aliveCheck)` ortak ve sınırlı bekleme döngüsüdür.
- `GatewayProbe`: JDK `HttpClient` (HTTP/1.1, connect 2 s, request 5 s) ile şunları yapar: `readiness()`, `verifyConnectionMode(expectedTag)`, `snapshot()`, `awaitIdle()` (`active_streams` ve `queue_depth` için ≤10 s, sonuç kaydedilir).
- `SimulatorStatsClient`: `Grpc.newChannelBuilder("127.0.0.1:"+port, InsecureChannelCredentials.create())` ve blocking stub kullanır, deadline 2 s. Sonuç bir map ya da `unavailable(error)` olur. Kanal `shutdownNow` + `awaitTermination(5s)` ile kapatılır.
- `EnvironmentCapture` + `CommandRunner` (arayüz) + `SystemCommandRunner` (timeout 15 s, çıktı 64 KiB ile sınırlı). Komutlar:
  - CPU modeli: Windows'ta `reg query "HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0" /v ProcessorNameString`, Linux'ta `/proc/cpuinfo` okunur, yedek olarak `PROCESSOR_IDENTIFIER`.
  - Physical core: Windows'ta `powershell -NoProfile -Command "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum"`, Linux'ta cpuinfo; olmazsa null.
  - RAM: `com.sun.management.OperatingSystemMXBean.getTotalMemorySize/getFreeMemorySize`.
  - Güç planı: Windows'ta `powercfg /getactivescheme`.
  - TIME_WAIT: Windows'ta `netstat -an -p tcp`, diğerlerinde `ss -tan`. Toplam ve iki port için sayılır.
  - Git: `git rev-parse HEAD`, `git status --porcelain` (ilk 200 satır), `git log -1 --format=%cI`, `git check-ignore -q <outputDir>`.

  Her komut başarısız olursa değer null olur ve bayrak konur; hiçbiri ölümcül değildir.
- `CpuLoadSampler`: `getCpuLoad()` ile önce bir hazırlık okuması yapılır, ardından pencere boyunca (default 3 s, smoke 1 s) 500 ms aralıkla örnek alınır. Değerler dizi olarak kaydedilir; negatif değer null olur.
- `OperatorChecklist`: anahtarlar `otherAppsClosed`, `onAcPower`, `samePowerPlan`.
  - `ask` modu `System.console()` var ve `isTerminal()` true ise çalışır; değilse exit 2 ile "use --checklist yes|skip" mesajı verilir.
  - Her soruya en fazla 3 geçersiz cevap kabul edilir, sonra cevap `unanswered` sayılır.
  - `yes` modu hepsini `yes` olarak ve kaynağı `flag` olarak kaydeder. `skip` modu `unanswered` kaydeder.
- `LoadgenOutputInspector`: summary dosyasını ve JSONL'in ilk ve son satırını okur → `LoadgenFacts`.
- `RunFlags`: facts, snapshot farkları, process canlılığı ve CPU örneklerinden bayrak türetir.
- `json/Json`: iç içe yapıları destekleyen küçük bir parser (object, array, string, long, double, boolean, null) ve sıralı, girintili bir writer (loadgen'deki RFC 8259 escape kuralları). Jackson eklenmez: aynı lib simulator'ın classpath'i olduğu için kirlenir, ayrıca ADR-0004 emsali var. Loadgen'e bağımlılık da yoktur, çünkü `FlatJson` düz yapılarla sınırlı ve modüller birbirine bağlanır.
- `Manifest` (model, `LinkedHashMap` tabanlı) ve `ManifestStore` (synchronized atomik yazım; `markInterrupted` monoton çalışır).
- `BenchRunner`: orkestrasyon. Bütün dış etkiler arayüzler üzerinden enjekte edilir. `volatile aborting` bayrağı tutar.

### Protokol (pinlenmiş, manifest `parameters` altına kaydedilir)

- **Host:** her yerde `127.0.0.1` kullanılır. Gateway portu 8080, simulator portu 50051; ikisi de override edilebilir ve kaydedilir.
- **Gateway komutu:** `java -Xms1g -Xmx1g -XX:+UseG1GC -Xlog:gc:file=gateway-gc.log:uptime,time,level,tags:filecount=2,filesize=8M -jar <gateway.jar>` ve ardından açık property'ler:
  - `--server.port=8080 --spring.threads.virtual.enabled=true --server.tomcat.max-connections=8192 --server.tomcat.accept-count=100`
  - `--lab.connection-mode=<shared|per-request> --lab.upstream.target=127.0.0.1:50051 --lab.upstream.stream-deadline=60s`
  - `--lab.limits.max-active-streams=100 --lab.limits.overload-policy=reject --lab.limits.queue-wait=5s --lab.limits.max-waiting=100 --lab.limits.stream-queue-capacity=16`
  - `--lab.timeouts.first-chunk=30s --lab.timeouts.idle-between-chunks=30s`
  - `--lab.retry.max-attempts=3 --lab.retry.initial-backoff=100ms --lab.retry.max-backoff=2s`
  - `--lab.shutdown.drain-timeout=10s --lab.shutdown.channel-termination-timeout=5s`

  Çalışma dizini run dizinidir; bu sayede GC log path'i göreli kalır ve Windows'taki `C:` iki noktası `-Xlog` sözdizimini bozmaz. İki run arasında yalnızca `lab.connection-mode` farklıdır.
- **Simulator:** `java -Xms512m -Xmx512m -XX:+UseG1GC -Xlog:gc:file=simulator-gc.log:... -cp <list> io.github.cemharput.streamlab.simulator.SimulatorMain`. Env: `SIMULATOR_PORT=50051`, `SIMULATOR_SHUTDOWN_GRACE_SECONDS=10`, `SIMULATOR_SCHEDULER_THREADS=<availableProcessors>` (açıkça yazılır ve kaydedilir).
- **Loadgen:** `java -Xms1g -Xmx1g -XX:+UseG1GC -Xlog:gc:file=<phase>-loadgen-gc.log:... -jar <loadgen.jar>` ve argümanlar:
  - `--target http://127.0.0.1:8080/stream --concurrency C --duration D --ramp-up R`
  - `--chunks 40 --interval-ms 50 --first-delay-ms 0 --fault NONE --fault-at-seq 0 --fault-rate 1.0 --seed 42`
  - `--connect-timeout 5s --idle-timeout 90s --drain-timeout 30s --reject-pause 100ms`
  - `--output <phase>.jsonl --summary-output <phase>-summary.json`

  Değerler sabit, aralık yoktur.
- **Seed politikası:** her run ve mode için aynı seed (42) kullanılır, değiştirilmez. Parametreler sabit değer olduğu için seed yalnızca sentetik metni belirler; her mode ve repetition aynı istekleri alır.
- **Akış uzunluğu:** 40 × 50 ms ≈ 2 s. Gerekçe: per-request mode akış başına bir TCP bağlantısı açar. 2 s'lik akışla c=150'de bile en fazla yaklaşık 50 bağlantı/s olur (izinli 100 akış / 2 s). Bir ölçüm ve warm-up yaklaşık 4000 bağlantı eder. ABBA'daki ardışık iki per-request run'ı bile Windows'un yaklaşık 16k ephemeral aralığının altında kalır; bu bir varsayım, TIME_WAIT sayısı bu yüzden kaydedilir.
- **Concurrency sweep (default):** `50,100,150`, `max-active-streams=100`'e göre seçildi:
  - 50, sınırın altı.
  - 100, tam sınır. Kapalı modelde permit terminal yazımından önce bırakıldığı için ret beklenmez; uç durumdur.
  - 150, sınırın 1,5 katı. Ret yolu (`REJECTED`, `rejected_total{reason=limit}`) görünür olur, `reject-pause` ile hot-loop sınırlanır.
- **Default değerler:** repetitions 4 (her mode, her concurrency için 4 ölçüm; `ABBA ABBA`, iki mode her pozisyonda eşit sayıda). Duration 60s, warm-up 20s (aynı concurrency ile), ramp-up 5s, run'lar arası cooldown 30s, CPU örnek penceresi 3s. 24 run, nominal olarak yaklaşık 1 saat; dry-run tahmini yazdırır.
- **Smoke preset:** concurrency `4`, repetitions 2 (ABBA = 4 run), duration 3s, warm-up 2s, ramp-up 0s, cooldown 1s, CPU penceresi 1s. Pinlenmiş diğer değerler aynıdır. Run-id `-smoke` ekiyle biter ve `SMOKE_PRESET` bayrağı konur.
- **Run başına prosedür:**
  1. İki portta dinleyici olmadığı kontrol edilir; TIME_WAIT sayısı kaydedilir.
  2. Simulator başlatılır, hazır olması beklenir.
  3. Gateway başlatılır, readiness ve mode doğrulanır.
  4. Snapshot `ready` alınır.
  5. Warm-up loadgen koşturulur (bekleme sınırı duration + 30 s drain + 20 s + 30 s pay), çıktısı incelenir.
  6. Gateway'in boşalması beklenir, snapshot `pre` alınır.
  7. CPU örneği alınır.
  8. Ölçüm loadgen'i koşturulur, çıktısı incelenir.
  9. Gateway'in boşalması ve simulator'da `active_generations==0` olması beklenir (≤10 s'lik polling), snapshot `post` alınır, process canlılığı kontrol edilir.
  10. Process'ler durdurulur: önce gateway (graceful bekleme 45 s, ardından forced 10 s), sonra simulator (20 s / 10 s). Dinleyicilerin kapandığı doğrulanır (≤30 s).
  11. Manifest güncellenir, cooldown beklenir.

  Ölçüm sırasında runner polling yapmaz. Run hatası durumunda run `failed` olur, process'ler temizlenir, kalan run'lar `not_run` olur ve exit 1 döner.
- **Snapshot içeriği (`snapshot-<phase>.json`):**
  - Gateway, her giriş için `{path, status, value, raw}` ile şu path'ler:
    - `/actuator/health/readiness`, `/actuator/metrics/active_streams`
    - `queue_depth?tag=queue:chunks|admission`
    - `rejected_total` (toplam ve 5 reason için `?tag=reason:<r>`)
    - `timeouts_total?tag=kind:first_chunk|idle`, `retries_total?tag=reason:restart|resume`, `retry_exhausted_total`, `connection_mode`
    - Bağlam için `jvm.memory.used?tag=area:heap`, `jvm.threads.live`, `process.cpu.usage`, `system.cpu.usage`

    404 hata değildir, kaydedilir.
  - Simulator: 8 GetStats alanı ya da `unavailable`.
  - `capturedAt` (wall clock).

### Layout ve şema

```
bench/results/<run-id>/
  manifest.json
  runs/<NNN>-c<C>-<mode>-r<R>/
    simulator.log  simulator-gc.log*  gateway.log  gateway-gc.log*
    warmup.jsonl  warmup-summary.json  warmup-loadgen.log  warmup-loadgen-gc.log*
    measured.jsonl  measured-summary.json  measured-loadgen.log  measured-loadgen-gc.log*
    snapshot-ready.json  snapshot-pre.json  snapshot-post.json
```

**`manifest.json` (`schemaVersion` 1):**
- Kimlik ve durum: `schemaVersion`, `protocolVersion`, `runId`, `tool="streamlab-bench"`, `toolVersion`, `status` (`running|completed|failed|interrupted`), `createdAt`, `finishedAt`.
- Not: `note="Raw data only. No analysis, no performance claim."`.
- Çağrı: `preset`, `commandLine`.
- `operatorChecklist{source, answers{…}, note}`.
- `environment{os, cpu{model, modelSource, logicalProcessors, physicalCores}, memory{totalBytes, freeBytesAtStart}, java{version, vendor, vmName, vmVersion, home}, git{commit, shortCommit, dirty, commitTime, statusLines}, powerPlan, timeWaitAtStart, removedEnvironmentVariables}`.
- `artifacts{gatewayJar, loadgenJar, loadgenLib[], simulatorClasspath[], benchJar}`; her biri `{path, sizeBytes, sha256, lastModified}`.
- `parameters{gateway{properties}, simulator{env}, jvm{gateway[], simulator[], loadgen[]}, loadgen{…}, plan{concurrency[], repetitions, durationMs, warmupMs, rampUpMs, cooldownMs, modeOrder[], alternation="ABBA", seed, ports, timeouts{…}, logLimitBytes, cpuBusyThreshold}}`.
- `runs[]`: `{index, id, dir, mode, concurrency, repetition, orderLabel, status, startedAt, endedAt, failure{phase, message}|null, processes{simulator|gateway: {pid, command[], envOverrides, startedAt, stop{method, exitCode, durationMs}, log{file, bytesWritten, bytesDropped}}}, timeWaitBefore, cpuLoadBefore[], warmup{warmup:true, loadgen{pid, command[], exitCode, durationMs, files{jsonl, summary, log}}, outcomes{…}, footer{stopReason, requestsStarted, recordsWritten, interrupted}, flags[]}, measured{same, warmup:false}, snapshots{ready, pre, post}, portsFreed, flags[]}`.
- `flags[]` (run-set düzeyi).

**Bayrak sözlüğü** (sabit string'ler; yalnızca kayıttır):
- **Loadgen çıktısı:**
  - `OUTCOME_<NAME>_NONZERO`: `COMPLETED` ve `REJECTED` dışındaki her `outcome_*` > 0 olduğunda. Generic olduğu için CONNECT_FAILURE/RESET/TIMEOUT/PROTOCOL_ERROR dahil, loadgen'e yeni bir outcome eklenirse kayma olmaz.
  - `REJECTED_AT_OR_BELOW_LIMIT`, `LOADGEN_EXIT_NONZERO`, `LOADGEN_WAIT_EXCEEDED`, `SUMMARY_MISSING`, `JSONL_HEADER_UNEXPECTED`, `JSONL_FOOTER_MISSING`, `STOP_REASON_NOT_DURATION`, `FOOTER_INTERRUPTED_NONZERO`.
- **Gateway ve simulator durumu:**
  - `ACTIVE_STREAMS_NOT_ZERO_BEFORE|AFTER`, `QUEUE_DEPTH_NOT_ZERO_AFTER`.
  - `REJECTED_REASON_<R>_DELTA_NONZERO`: limit dışındaki reason'lar için.
  - `TIMEOUTS_DELTA_NONZERO`, `RETRIES_DELTA_NONZERO`, `RETRY_EXHAUSTED_DELTA_NONZERO`.
  - `SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER`, `SIMULATOR_FAULTED_DELTA_NONZERO`, `SIMULATOR_REJECTED_DELTA_NONZERO`, `SIMULATOR_STATS_UNAVAILABLE`.
  - `GATEWAY_EXITED`, `SIMULATOR_EXITED`, `CONNECTION_MODE_MISMATCH`, `PORT_NOT_FREED`.
- **Ortam ve set düzeyi:**
  - `CPU_BUSY_BEFORE_RUN`: ortalama > 0,25. Eşik sabittir, kaydedilir ve bir işaretleyicidir, yargı değil.
  - `CPU_LOAD_UNAVAILABLE`, `TIME_WAIT_UNAVAILABLE`, `LOG_TRUNCATED`.
  - Set düzeyi: `GIT_DIRTY`, `GIT_UNAVAILABLE`, `ARTIFACT_OLDER_THAN_HEAD`, `OUTPUT_NOT_GITIGNORED`, `CHECKLIST_NOT_CONFIRMED`, `CHECKLIST_ANSWER_NO`, `SMOKE_PRESET`, `INTERRUPTED`.

**Commit edilen örnek dosya yok.** Uydurma sayılar sonuç sanılabilir. Şema `docs/benchmark-protocol.md` içinde tablo olarak yazılır, `ManifestTest` de anahtar setini sabitler.

**Arşivleme belgelenir:** `tar -czf bench-<run-id>.tar.gz -C bench/results <run-id>` (Git Bash ve Windows 11 `tar.exe`) ya da `Compress-Archive`. Arşiv bir release ya da issue'ya eklenir, commit edilmez.

### CLI

| Seçenek | Default (smoke) | Doğrulama |
|---|---|---|
| `--preset default\|smoke` | default | enum |
| `--repetitions N` | 4 (2) | 1..20 |
| `--duration D` / `--warmup D` | 60s / 20s (3s / 2s) | 1s..2h; `<int>ms\|s\|m` |
| `--ramp-up D` | 5s (0s) | ≥0, `< min(duration, warmup)` |
| `--concurrency LIST` | 50,100,150 (4) | 1..10000, en fazla 10 değer, tekrarsız |
| `--mode-order A,B` | shared,per-request | iki mode tam birer kez |
| `--cooldown D` | 30s (1s) | 0..10m |
| `--output-dir PATH` | `<repo-root>/bench/results` | yoksa oluşturulur; dosya olamaz |
| `--repo-root PATH` | cwd | `pom.xml`, `gateway/` ve `loadgen/` içermeli |
| `--gateway-jar`, `--loadgen-jar`, `--simulator-classpath` | auto-detect | mevcut; auto-detect tam olarak bir eşleşme bulmalı |
| `--gateway-port`, `--simulator-port` | 8080, 50051 | 1..65535, farklı |
| `--checklist ask\|yes\|skip` | ask | enum |
| `--operator-note TEXT` | yok | ≤ 500 karakter |
| `--dry-run`, `--help`/`-h` | – | bayrak |

### Bulgular (mevcut modüller; değiştirilmez, raporlanır)

- **F1:** Simulator'ın çalıştırılabilir artefaktı yok. README yalnızca `exec:java` ile başlatmayı gösteriyor, bu da Maven JVM'inde çalışmak demek. Harness classpath'i `bench/target/lib`'den kurar.
- **F2:** Simulator istatistikleri yalnızca gRPC üzerinden okunabiliyor, HTTP yüzeyi yok. Harness proto sınıflarıyla küçük bir GetStats client'ı kullanır.
- **F3:** Windows'ta gateway ve simulator'ın graceful shutdown'u dışarıdan tetiklenemez. `Process.destroy` `TerminateProcess` çağırır ve actuator `shutdown` endpoint'i açık değil. Veri toplandıktan sonra process'ler sert öldürülür; shutdown yolları protokolün parçası değildir. Linux'ta SIGTERM gider.
- **F4:** `server.port` yaml'da yok (8080 örtük). `lab.upstream.target` `localhost`; IPv4/IPv6 çözümleme varyansını önlemek için harness `127.0.0.1` pinler.
- **F5:** `SIMULATOR_SCHEDULER_THREADS` varsayılanı makineye bağlı; harness açıkça set eder.
- **F6:** Gateway chunk boyutunu (`chunk_size_bytes`) dışarı açmıyor, payload sabit olarak tek kelime.
- **F7:** Loadgen exit 0 outcome'ları yansıtmaz; harness summary ve JSONL'i inceler (beklenen davranış).

## 3. Uygulama adımları

1. Root `pom.xml`'e modül eklenir, `bench/pom.xml` yazılır. `./mvnw -B -pl bench -am package -DskipTests` sonrası `bench/target/lib` içeriği ve jar manifest'i kontrol edilir.
2. `json/Json` ve `JsonTest`.
3. `BenchOptions`, `BenchOptionsParser`, `Preset`, `ConnectionMode`, `UsageException` ve `BenchOptionsParserTest`.
4. `PlanBuilder`, `RunPlan`, `PlannedRun`, `RunId`, `ResultLayout` ve testleri.
5. `Artifacts`, `ProcessSpecs` ve testleri (komut satırı golden'ları, env sanitizasyonu).
6. `ChildProcess`, `JdkChildProcess`, `ProcessLauncher`, `ProcessSupervisor`, `BoundedLogPump`, `PortProbe`, `Ticker` ve testleri; gerçek child JVM testleri dahil.
7. `Readiness`, `GatewayProbe`, `SimulatorStatsClient` ve testleri (HttpServer ile sahte actuator, port 0'da sahte admin servisli Netty gRPC sunucusu).
8. `EnvironmentCapture`, `CommandRunner`, `SystemCommandRunner`, `CpuLoadSampler`, `OperatorChecklist` ve testleri.
9. `LoadgenOutputInspector`, `RunFlags`, `Manifest`, `ManifestStore` ve testleri.
10. `BenchRunner`, `BenchMain` (dry-run yazıcısı, hook) ve `BenchRunnerTest`, `BenchMainTest`.
11. Dokümanlar:
    - `docs/benchmark-protocol.md` (İngilizce; ilk satırda "Protocol version: 1").
    - `docs/adr/0005-benchmark-runner.md`.
    - `README.md`: durum paragrafı, modül satırı `bench/`, "Benchmark protocol" bölümü (build, dry-run, smoke ve default komutları, `--checklist`, Windows notları: Ctrl+C için PowerShell/Windows Terminal; mintty davranışı doğrulanmadı), "This harness and its results make no performance claim; it compares nothing by itself", arşivleme, Decisions'a ADR-0005.
    - `.gitignore`'a `bench/results/`.
12. Odaklı test, 3 kez `verify`, ardından manuel smoke ve hata/kesinti kontrolleri (Bölüm 4).

Test dosyaları `bench/src/test/java/io/github/cemharput/streamlab/bench/` altında: `JsonTest`, `BenchOptionsParserTest`, `PlanBuilderTest`, `RunIdTest`, `ResultLayoutTest`, `ArtifactsTest`, `ProcessSpecsTest`, `BoundedLogPumpTest`, `PortProbeTest`, `ProcessSupervisorTest` (fake'ler), `ProcessSupervisorRealProcessTest`, `GatewayProbeTest`, `SimulatorStatsClientTest`, `EnvironmentCaptureTest`, `OperatorChecklistTest`, `LoadgenOutputInspectorTest`, `RunFlagsTest`, `ManifestTest`, `BenchRunnerTest`, `BenchMainTest`, `ProtocolDocConsistencyTest` (`../docs/benchmark-protocol.md` içindeki "Protocol version: 1" ile sabiti karşılaştırır). Yardımcılar `testsupport/` altında: `FakeChildProcess`, `FakeLauncher`, `FakeActuatorServer`, `ChildMain` (yalnızca JDK kullanır; latch ile bloklanır, istenirse bir torun process başlatır, istenirse bloklayan bir shutdown hook kurar). Child classpath'i surefire booter jar'ı değil, `ChildMain`'in code-source'u (`target/test-classes`) olur.

## 4. Doğrulama

| AC | Kontrol | Beklenen |
|---|---|---|
| AC1 | 3× `./mvnw -B verify`; `./mvnw -B -pl bench -am dependency:tree`; `git diff --stat -- gateway simulator proto loadgen` | yeşil; yalnızca proto, grpc ve simulator kapanışı; diff boş |
| AC2 | `BenchOptionsParserTest`, `BenchMainTest` | hata durumlarında exit 2 ve dizin yok; help 0 |
| AC3 | `PlanBuilderTest` | N=1..4 için `AB`, `ABBA`, `ABBAAB`, `ABBAABBA`; ters mode-order; tekil id'ler |
| AC4 | `BenchMainTest` (`@TempDir`, sahte artefaktlar); manuel dry-run | bütün run'lar ve komutlar yazdırılır; launcher çağrılmaz; dosya oluşmaz |
| AC5 | `RunIdTest`, `ResultLayoutTest`; manuel ikinci smoke | format doğru; çakışmada `-2`; ilk dizin değişmez |
| AC6 | `ManifestTest`, `BenchRunnerTest` | anahtar seti; tmp dosya kalmaz; durum geçişleri doğru |
| AC7 | `GatewayProbeTest`, `SimulatorStatsClientTest`, `BenchRunnerTest` | hazırlık, timeout, mode uyuşmazlığı ve erken çıkışta hızlı hata |
| AC8 | `RunFlagsTest`, `LoadgenOutputInspectorTest` | beklenen bayraklar; veri silinmez |
| AC9 | `EnvironmentCaptureTest`, `OperatorChecklistTest` | komutlar başarısız olunca null ve bayrak; checklist modları |
| AC10 | `ProcessSupervisorTest`, `ProcessSupervisorRealProcessTest` (child ve torun ölü; Linux'ta bloklayan hook'tan forced'a geçiş), `BenchRunnerTest` (hazırlık hatası ve hook ile kesinti: hepsi durdurulur, yeni start reddedilir), `BoundedLogPumpTest`; manuel kontroller | yetim process yok, port boş |
| AC11 | manuel smoke | exit 0, iki mode, manifest şekli |
| AC12 | doküman incelemesi; `git check-ignore bench/results/x` | içerik tam; sayı yok; ignore ediliyor |
| AC13 | kaynakta grep (`Thread.sleep`, `@Timeout`); surefire süreleri | temiz; yaklaşık 30 s altı |

**Komutlar** (repo kökünden; PowerShell'de `.\mvnw.cmd`):
- Odaklı: `./mvnw -B -pl bench -am verify -Dtest='io/github/cemharput/streamlab/bench/**/*Test' -Dsurefire.failIfNoSpecifiedTests=false`
- `./mvnw -B verify` (3 kez)
- Build: `./mvnw -B package -DskipTests`
- Dry-run: `java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --dry-run` ve `... --preset smoke --dry-run`
- Smoke: `java -jar bench/target/bench-0.1.0-SNAPSHOT.jar --preset smoke --checklist yes`

**Manuel smoke prosedürü (implementer; yalnızca şekil raporlanır, sayı raporlanmaz):**
1. Smoke çalıştırılır; exit kodu kaydedilir.
2. `Get-ChildItem -Recurse bench/results/<id>` (ya da `ls -R`) ile dizin listesi alınır.
3. Manifest'in üst düzey anahtarları, run sayısı, mode/status/orderLabel ve bayrak adları raporlanır.
4. Manifest'teki PID'ler için `Get-Process -Id <pid>` hata vermeli. `Get-NetTCPConnection -LocalPort 8080,50051 -State Listen` boş dönmeli.
5. `git status` içinde `bench/results` görünmemeli.
6. İkinci smoke çalıştırılır: yeni dizin oluşmalı, ilk manifest'in sha256'sı değişmemeli.
7. Port dolu senaryosu: PowerShell'de `$l=[System.Net.Sockets.TcpListener]::new([Net.IPAddress]::Any,8080); $l.Start()` ile port tutulur. Smoke exit 2 vermeli ve dizin oluşmamalı.
8. Run hatası senaryosu: `--gateway-jar` loadgen jar'ını gösterecek şekilde verilir. Status `failed` olmalı, simulator ölü olmalı, exit 1 dönmeli.
9. Kesinti: PowerShell'de `--duration 30s` ile ölçüm sırasında Ctrl+C basılır. Status `interrupted` olmalı, PID'ler ölü ve portlar boş olmalı. Aynısı Git Bash'te denenir; sonuç ne olursa olsun bulgu olarak raporlanır.

**Riskler:**
- **Windows'ta process temizliği:** graceful stop yok (F3). Runner sert öldürülürse yetim process kalabilir; azaltma önlemleri PID kaydı ve port ön kontrolü.
- **TIME_WAIT ve port tükenmesi:** per-request mode'un bağlantıları sonraki run'a taşınabilir. Azaltma önlemleri 2 s'lik akışlar, cooldown, ABBA sırası ve TIME_WAIT kaydı. Kanıt değil, varsayım.
- **Port probe:** Windows'ta kapalı bir loopback portuna connect yaklaşık 2 s sürebilir; timeout 3 s.
- **JIT/GC varyansı:** azaltma önlemleri her run'da taze JVM, warm-up, sabit heap, G1 ve GC log'ları. Arka plan yükü (Defender, indeksleme) checklist ve CPU örneğiyle yalnızca işaretlenir.
- **Disk:** c=150'de 503 kayıtları JSONL'i büyütür; default set yüzlerce MB tutabilir. Log'lar 16 MiB, GC log'ları 2×8 MiB ile sınırlı.
- **Flaky testler:** testler süre değeri assert etmez. Gerçek process testleri en fazla 3 child JVM başlatır ve Awaitility `atMost` en az 5 kat pay taşır.
- **Bilinçli yinelemeler:** elle yazılan JSON parser'ı bir risk; girdiler bilinen üreticilerden geliyor. Kendi kodu aynı makinede küçük bir ek yük.

**Varsayımlar (smoke'ta ya da ilk build'de doğrulanacak):**
- **A1:** Boot 4.1 metrics endpoint'i `measurements[].statistic/value` ve `availableTags` döner, `?tag=k:v` destekler.
- **A2:** Windows'ta `destroy` ile `destroyForcibly` eşdeğerdir (`TerminateProcess`).
- **A3:** reactor `package` fazında `copy-dependencies` simulator jar'ını alır.
- **A4:** `getCpuLoad()` Windows 11'de [0,1] aralığında değer döner.
- **A5:** `reg query`, `powercfg` ve `netstat` admin yetkisi olmadan çalışır.
- **A6:** mintty altında `Console.isTerminal()` false döner, bu yüzden `--checklist` gerekir.
- **A7:** çocukların konsoldan CTRL_C alıp almadığı bilinmiyor; hook her iki durumda da temizler.

**Defekt durumunda:** smoke gateway, simulator ya da loadgen'de bir sözleşme ihlali gösterirse (ör. mode tag'inin yanlış olması, readiness'ın hiç UP olmaması, footer'ın eksik olması) iş durur. Kanıt (manifest girdisi, log satırı, komut) ile rapor edilir, feature `blocked` olur. O modüller değiştirilmez.

**Açık sorular** (bloklayıcı değil; varsayılan kararlar alındı):
- **S1:** default set 24 run ve yaklaşık 1 saat. Repetitions 4 yerine 2 tercih edilebilir.
- **S2:** Windows'ta graceful stop için actuator `shutdown` endpoint'i komut satırı override'ı ile açılmadı; config gateway default'larına yakın kalıyor. Bu tercih edildiği için seçilmedi; isteğe göre açılabilir.
- **S3:** gizlilik için `hostName` kaydedilmiyor.
