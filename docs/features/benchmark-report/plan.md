# Plan: benchmark-report

Doğrulanmış olgular (HEAD 37a09b7; dosyalar doğrudan okundu, code-graph MCP kullanılmadı):

- **Build:** root `pom.xml` modülleri sırasıyla `proto, simulator, gateway, loadgen, bench`. `hdrhistogram.version` 2.2.2 `dependencyManagement` içinde tanımlı. `pluginManagement` içinde jar 3.4.2, dependency 3.8.1 ve surefire 3.5.4 var. JaCoCo 0.8.15 her modüle otomatik uygulanıyor. CI ubuntu + temurin 26 ile `./mvnw -B verify` çalıştırıyor, timeout 15 dk. `.gitattributes` yok, hiçbir modülde `src/test/resources` yok.
- **bench/target/lib = simulator classpath:** `Artifacts.resolve` varsayılan olarak `bench/target/lib` içindeki **bütün** jar'ları simulator'ın `-cp` listesine ve manifest'teki `artifacts.simulatorClasspath` alanına koyuyor. `bench`'e HdrHistogram eklenirse pinlenmiş simulator classpath'i ve protokol artefakt listesi değişir.
- **`bench.json.Json`:** `parse(String)` / `parseObject` / `write` / `writeCompact` sunuyor. Sayılar `Long` ya da `Double` olarak okunuyor. **Tuzak:** writer `Double`/`Float` dışındaki her `Number` için `longValue()` yazıyor; `BigDecimal` verilirse kesilir.
- **Bayrak sözlüğü:** `Flags.DOCUMENTED` 37 giriş, iki desen içeriyor: `OUTCOME_<NAME>_NONZERO` ve `REJECTED_REASON_<R>_DELTA_NONZERO`. Reason değerleri `GatewayProbe.REJECTED_REASONS` içinde: `limit, queue_timeout, shutdown, upstream_unavailable, interrupted`. Bayraklar üç yerde duruyor:
  - set düzeyi: kök `flags`,
  - run düzeyi: `runs[].flags` (CPU, snapshot, process, port, log),
  - faz düzeyi: `runs[].warmup.flags` ve `runs[].measured.flags` (loadgen bayrakları, log).
- **Manifest gerçeği:** `measured.footer` yalnızca `stopReason, requestsStarted, recordsWritten, interrupted` tutuyor. `files` alanında sabit adlar var (`measured.jsonl`, `measured-summary.json`). `dir` her zaman `/` ile yazılıyor (`ResultLayout.relativeRunDir`). Mutlak yollar şu alanlarda: `artifacts.*.path`, `java.home`, `processes.*.command`, loadgen `command`. `git.statusLines` göreli dosya adları içeriyor.
- **schemaVersion 1 sürüm artırılmadan değişmiş:** eski smoke manifest'lerinde `stop.method` değeri `graceful`; bir başka manifest'te `log` içinde `bytesLostAfterWriteError`/`writeError` yok. Güncel değerler `already_exited | destroy | forced | survived` (`ProcessSupervisor`). İstekte geçen "destroyForcibly" kayıtta `forced` olarak görünüyor.
- **Linux ayrımları (`EnvironmentCapture`):**
  - `cpu.modelSource` `/proc/cpuinfo model name` ya da `unavailable` ya da `env PROCESSOR_IDENTIFIER` olabiliyor.
  - `physicalCores` cpuinfo'daki `core id`'den geliyor, null olabilir.
  - `powerPlan` Windows dışında her zaman `null`.
  - TIME_WAIT `ss -tan` ile sayılıyor (`TIME-WAIT`).
- **Loadgen JSONL:**
  - İçerik: header, request satırları (bitiş sırasına göre, index sırası değil), footer.
  - Alanlar ns cinsinden tam sayı. `ttfbNanos`, `firstBodyByteNanos`, `ttfcNanos` null olabiliyor.
  - `totalNanos` her zaman var. Outcome anı `startOffsetNanos + totalNanos`.
  - Footer'da `elapsedNanos, stopReason, requestsStarted, recordsWritten, interrupted, interChunkCount, interChunkHistogramUnit="ns"` ve `interChunkHistogram` (bir `ConcurrentHistogram(3)` + `encodeIntoCompressedByteBuffer` + Base64) var.
  - Inter-chunk sink bütün akışlar için her chunk'ta (ilki hariç) çağrılıyor. Kayıttaki `chunks` alanı karar anındaki snapshot olduğu için iptal edilen akışlarda `Σ(chunks−1)` değeri `interChunkCount`'tan farklı olabilir.
  - `Summary.record` her drenaj kaydı için çağrılıyor; temiz bir run'da summary ile JSONL sayıları eşittir.
- **Snapshot dosyaları:** `gateway` bir `{path,status,value,raw}` listesi (path'ler `GatewayProbe` sabitlerinden), `simulator` 8 alan ya da `{unavailable}`. Taze JVM'ler yüzünden sayaçlar her run'da 0'dan başlıyor.
- **Smoke dizinleri** (yalnızca şekil için okundu, veri olarak kullanılmayacak): 8 set var; içlerinde `completed`, `failed` (+`not_run`) ve `interrupted` olanlar bulunuyor.

## 1. Davranış ve kabul kriterleri

Yeni Maven modülü `report` eklenir. İçindeki düz Java aracı `ReportMain` tek bir sonuç setini (`bench/results/<run-id>/` biçiminde) yalnızca okur ve şunları üretir:
- tamamen üretilmiş bir `docs/benchmark-report.md`,
- küçük türetilmiş veriler: `docs/benchmark/` altında CSV, JSON ve SVG.

Araç yalnızca betimleyici istatistik verir. p-değeri, güven aralığı ya da sıralama vermez. Mod karşılaştırmasını yalnızca aynı concurrency içinde, kapalı bir cümle şablonu kümesiyle yazar. Sonuç seti yoksa aynı araç sayı içermeyen bir yer tutucu rapor üretir.

### Mimari karar: yeni modül `report` (bench'e alt komut değil)

- **bench'e alt komut reddedildi.** Footer histogramını çözmek için HdrHistogram gerekiyor. Bu, `bench/target/lib`'e, yani simulator classpath'ine ve manifest'teki `simulatorClasspath`'e girer; pinlenmiş protokol artefakt listesi değişir. Runner ile analiz aynı jar'da karışır ve bench'e dokunmak gerekir (yasak).
- **HdrHistogram'ı elle çözmek reddedildi.** Formatın kopyası çıkar ve riskli olur.
- **Python/R reddedildi.** Yeni bir runtime gerekir.
- **Seçilen yapı:** `report`, `bench`'e `<exclusions><exclusion><groupId>*</groupId><artifactId>*</artifactId></exclusion></exclusions>` ile bağımlı olur; böylece yalnızca bench jar'ı gelir. Ayrıca yönetilen sürümle `org.hdrhistogram:HdrHistogram` bağımlılığı eklenir. `report/target/lib` = `bench-0.1.0-SNAPSHOT.jar` + `HdrHistogram-2.2.2.jar`.
  - Yeniden kullanılanlar: `Json`, `Flags`, `Manifest` sabitleri, `ResultLayout` sabitleri, `GatewayProbe` path sabitleri, `ConnectionMode`, `PlanBuilder` (beklenen ABBA planıyla çapraz kontrol için).
  - Bu sınıflar yalnızca JDK kullanıyor. Bunun gRPC'siz bir classpath'te çalıştığı, BoundedMemoryTest'in child JVM'iyle ve manuel `java -jar` ile doğrulanır.
  - Bench jar'ının manifest'indeki `Class-Path: lib/...` girdileri `report/target/lib/lib/` altında çözülemez; eksik Class-Path girdileri JVM tarafından yok sayılır.
- **Test kapsamı:** `loadgen` bağımlılığı (gerçek üretici sözleşme testi ve `Outcome` adları için) ve junit/assertj eklenir.
- **Paketleme:** jar + `lib/` (bench/loadgen'deki desenle aynı). Giriş noktası: `java -jar report/target/report-0.1.0-SNAPSHOT.jar`.
- Gerekçe ADR-0006'ya yazılır.

### Kabul kriterleri

- **AC1 (build/sınırlar):**
  - Root `pom.xml`'e `<module>report</module>` eklenir (bench'ten sonra).
  - `./mvnw -B verify` art arda 3 kez yeşil geçer.
  - `git diff --stat -- bench gateway simulator proto loadgen` boş kalır.
  - `report/target/lib` yalnızca bench jar'ı ve HdrHistogram'ı içerir (`dependency:tree` ve dizin listesiyle kontrol edilir).
  - Testler gateway/simulator/loadgen process'i başlatmaz. Tek child process, BoundedMemoryTest'te analizörün kendisidir.
- **AC2 (girdi sözleşmesi ve sürüm):**
  - Aşağıdaki durumlar exit 2 verir; hiçbir dosya yazılmaz:
    - `schemaVersion ≠ 1`,
    - `protocolVersion ≠ 1`,
    - `tool ≠ streamlab-bench`,
    - manifest okunamıyor ya da obje değil,
    - zorunlu anahtar eksik ya da tipi yanlış.
  - Loadgen header'ının ya da summary'nin `schemaVersion`'ı 1 değilse yalnızca o run dışlanır.
  - Opsiyonel alanların eksik olması tolere edilir (eski log anahtarları, bilinmeyen `stop.method`).
  - Geçersiz `dir`/dosya adları (`..`, mutlak yol, sembolik bağla kök dışına çıkış) asla okunmaz; run dışlanır.
- **AC3 (sınıflandırma ve görünürlük):**
  - Bölüm 2'deki tabloya göre `Flags.DOCUMENTED`'daki her giriş exclude, caution ya da info sınıfındadır. Bilinmeyen bayrak exclude sayılır.
  - Status'u `completed` olmayan her run dışlanır.
  - Her run raporun envanter tablosunda sınıfı (`usable | caution | excluded`) ve gerekçe kodlarıyla görünür. Stdout dışlananları listeler ve son satır makine okunur bir JSON'dır.
  - Warm-up fazları listelenir, analiz edilmez.
- **AC4 (metrikler):** Bölüm 2'deki tanımlar uygulanır. Elle hesaplanmış testler geçer:
  - nearest-rank yüzdelikler,
  - HdrHistogram footer çözümü,
  - pencere tabanlı tamamlanma hızı,
  - oranlar,
  - snapshot delta'ları.
- **AC5 (karşılaştırma):**
  - Karşılaştırma yalnızca concurrency içinde yapılır.
  - Repetition başına değerler yan yana gösterilir; yanlarında run index'i ve ABBA pozisyonu (1./2.) bulunur.
  - Mod başına medyan, min–max ve n verilir.
  - Örtüşme, yayılım ve yetersizlik kuralları şablonlu cümleyle yazılır. Caution run'larına bağımlılık ayrı bir cümleyle belirtilir.
  - Kovalanmış (pooled) yüzdelik yoktur.
- **AC6 (ifade kuralı):**
  - Yasaklı ifade taraması test edilir. Kapsam: üretilen rapor (bütün senaryolar), yer tutucu, `ReportText`, SVG/CSV/JSON metinleri, bütün `Statement` şablonları.
  - Muafiyet yalnızca işaretli ve protokolün 2. bölümüyle birebir aynı alıntı bloğu içindir.
  - Araç taramadan geçmeyen bir raporu yazmayı reddeder (exit 1).
- **AC7 (sınırlar):** Rapor protokolün "What it cannot answer" maddelerini birebir içerir, ayrıca ttfb, buffer çözünürlüğü, coordinated omission, reject-pause, aynı makine ve kapalı model hızı caveat'larını içerir.
- **AC8 (veri kalitesi bölümü):** Bölüm 2'deki çapraz kontroller (C1–C12), ortam, checklist, CPU örnekleri, TIME_WAIT, artefaktlar (ad, boyut, sha256), commit/dirty, durdurma kayıtları ve GC log'ları ("listed, not analysed") raporda yer alır.
- **AC9 (çıktılar ve yollar):**
  - Sabit dosya listesi yazılır.
  - Var olan bir dosyaya `--overwrite` olmadan yazılmaz (exit 2).
  - `--out` sonuç setinin içinde olamaz (exit 2).
  - Sonuç seti değişmez (dosya listesi, boyut, mtime ve sha256 öncesi/sonrası aynı).
  - `--placeholder` modu çalışır.
- **AC10 (determinizm):** Aynı girdi bayt-özdeş çıktı verir. Bu; `Locale` tr-TR/de-DE ve farklı `TimeZone` altında, kayıt sırası karıştırıldığında ve LF/CRLF girdilerde de geçerlidir. Çıktıda her zaman `\n` kullanılır. Manifest dışından zaman damgası eklenmez.
- **AC11 (gizlilik):**
  - Üretilen dosyalarda ve stdout'ta mutlak yol, kullanıcı adı, `java.home`, komut satırı, `operatorNote` metni, `failure.message`, `harnessFailure` metni ve `statusLines` yer almaz.
  - Testte sahte kullanıcı adı `fixtureuser` kullanılır.
  - Yazmadan önce çalışan bir `PrivacyGuard` vardır.
- **AC12 (sınırlı bellek):** `-Xmx32m` ile başlayan bir child JVM, en az 96 MB'lık JSONL'i `--check` ile exit 0 bitirir. 1 MiB'ı aşan bir satır bozuk sayılır ve atlanır.
- **AC13 (Windows'ta Linux seti):** Linux tarzı fixture'lar (yollar `/home/...`, `powerPlan: null`, cpuinfo kaynağı, `destroy` + exitCode 143, LF ya da CRLF) aynı metrikleri verir. Bilinmeyen `stop.method` info sayılır.
- **AC14 (CLI):** seçenekler, validasyon ve exit kodları: 0 başarı (dışlama olsa da), 2 kullanım ya da geçersiz girdi, 1 beklenmeyen hata (I/O, guard ihlali). `--check` hiçbir şey yazmaz. `--help` exit 0 döner.
- **AC15 (dokümanlar):**
  - README: durum paragrafı, modül satırı, "Benchmark report" bölümü (komutlar, "describes one result set on one machine; no performance claim", arşivden yeniden üretme), Decisions'a ADR-0006.
  - Protokol dokümanına çapraz referans eklenir. Protokol sürümü değişmez; `ProtocolDocConsistencyTest` yeşil kalır.
  - `docs/adr/0006-benchmark-report.md` eklenir.
  - `docs/benchmark-report.md` yer tutucu olarak ya da gerçek setten üretilir.
- **AC16 (hijyen):** Testlerde `Thread.sleep` yok. Her test sınıfında `@Timeout(threadMode = SEPARATE_THREAD)` var. `report` testleri toplamda yaklaşık 30 s altında kalır.
- **AC17 (manuel son adım):** Kullanıcının adını verdiği gerçek set analiz edilir; set yoksa yer tutucu kalır ve bu, kalan adım olarak raporlanır. Chat'e yalnızca dosya listesi ve yapı yazılır.
- **AC18 (defekt):** Analiz runner, loadgen, gateway ya da simulator'da bir defekt gösterirse iş durur ve kanıtıyla raporlanır. O modüller değiştirilmez.

## 2. Dosyalar, semboller ve tasarım

### Pom

- **Root:** `<module>report</module>` eklenir. Yeni property ya da sürüm eklenmez.
- **`report/pom.xml`:**
  - Bağımlılıklar:
    - `io.github.cemharput.streamlab:bench:${project.version}` (compile, `*:*` exclusion ile),
    - `org.hdrhistogram:HdrHistogram` (sürümsüz),
    - test kapsamında `io.github.cemharput.streamlab:loadgen:${project.version}`, `junit-jupiter`, `assertj-core`.
  - `maven-jar-plugin`: `mainClass=io.github.cemharput.streamlab.report.ReportMain`, `addClasspath`, `classpathPrefix=lib/`, `addDefaultImplementationEntries`.
  - `maven-dependency-plugin` `copy-dependencies`: `package` fazında, `includeScope=runtime`, `${project.build.directory}/lib`.

### Girdi sözleşmesi

- **Kök:** `--results <dir>` içinde normal bir dosya olarak `manifest.json` bulunmalı. Değilse exit 2 ve "pass the run-set directory" mesajı verilir. Boyut sınırları:
  - manifest en fazla 64 MiB,
  - snapshot en fazla 16 MiB,
  - summary en fazla 1 MiB.
- **Zorunlu anahtarlar (exit 2):**
  - kökte: `schemaVersion` (Long 1), `protocolVersion` (Long 1), `tool` (`streamlab-bench`), `runId` (String), `status` (String),
  - her run'da: `index, id, dir, mode, concurrency, repetition, orderLabel, status`.
  - `runs` yoksa boş liste sayılır (minimal `failed` manifest).
- **Okunan opsiyonel alanlar** (null ya da eksikse "not recorded" yazılır ve `CHECK_FIELD_MISSING` info olarak işlenir):
  - `preset, createdAt, finishedAt, toolVersion`, `operatorChecklist.{source,answers}`,
  - `environment.{os, cpu.*, memory.totalBytes, java.{version,vendor,vmName}, git.{commit,shortCommit,dirty,commitTime}, powerPlan, timeWaitAtStart, removedEnvironmentVariables}`,
  - `artifacts.*.{path→yalnızca basename, sizeBytes, sha256}`,
  - `parameters.{gateway.properties, jvm, loadgen.{target,fixedArguments}, plan.*}`,
  - run içinde: `failure.phase`, `processes.*.stop.{method,exitCode}`, `processes.*.log.{bytesDropped,bytesLostAfterWriteError}`, `timeWaitBefore`, `cpuLoadBefore`, `warmup/measured.{loadgen.{exitCode,files}, outcomes, footer, flags}`, `snapshots`, `portsFreed`, `flags`.
  - Okunan ama asla çıktıya konmayan alanlar: `operatorNote` (yalnızca varlığı yazılır), `git.statusLines` (yalnızca sayısı), `failure.message`, `harnessFailure` (yalnızca varlığı).
  - Hiç okunmayan alanlar: `commandLine`, `command[]`, `java.home`.
- **Run'a ait dosyalar:**
  - `dir` hem `/` hem `\` ile bölünür. Beklenen biçim `runs/<id>`; `id` deseni `^\d{3}-c\d+-(shared|per-request)-r\d+$` olmalı ve run'ın `id` alanına eşit olmalı.
  - Dosya adları manifest'ten değil `ResultLayout` sabitlerinden gelir. Manifest'teki `files` değerleri bu adlarla aynı değilse `CHECK_FILE_NAME_UNEXPECTED` olur.
  - Her path `toRealPath()` sonrasında kökün altında kalmalı.
  - Bütün dosyalar yalnızca okuma için açılır.
- **Okunan dosyalar:** `measured.jsonl` (stream), `measured-summary.json`, `snapshot-pre.json` ve `snapshot-post.json`. Varlık ve boyut için yalnızca listelenenler: `warmup.jsonl`, `warmup-summary.json`, `*-gc.log*`, `snapshot-ready.json`, `*.log`.
- **Satır okuma:** `BoundedLineReader` UTF-8 okur. `\n`, `\r\n` ve tek `\r` satır sonu sayılır. Satır başına üst sınır 1 MiB karakterdir; aşan satır atlanır ve bozuk sayılır.
- **Status:** yalnızca `completed` run'lar karşılaştırmaya girebilir. `failed | interrupted | not_run | planned | running` run'lar "status <x>" gerekçesiyle dışlanır. Measured dosyaları varsa metrikleri yine hesaplanır ve run tablosunda dışlanmış olarak gösterilir. Set status'u `completed` değilse kimlik bloğunda "set incomplete" yazılır.

### Bayrak sınıflandırması (`FlagPolicy`; ad + kaynak `SET | RUN | MEASURED | WARMUP`)

Kurallar:
- `WARMUP` kaynaklı her bayrak caution sayılır ("warm-up anomaly"); warm-up asla dışlatmaz.
- `SET` kaynaklı bir exclude bayrağı bütün run'ları dışlar. `SET` kaynaklı caution yalnızca kimlik bloğunda banner olarak görünür.

| Bayrak | Sınıf | Gerekçe |
|---|---|---|
| `OUTCOME_INTERRUPTED_NONZERO` | exclude | drain timeout akışları kesti, dağılımlar sansürlü |
| `OUTCOME_<diğer>_NONZERO` | caution | hatalar gözlemin parçası, oranlar raporlanır |
| `REJECTED_AT_OR_BELOW_LIMIT` | caution | beklenmeyen ret, oran raporlanır |
| `LOADGEN_EXIT_NONZERO`, `LOADGEN_WAIT_EXCEEDED` | exclude | loadgen düzgün bitmedi |
| `SUMMARY_MISSING` | caution | JSONL sonucun kendisi (ADR-0004), yalnızca çapraz kontrol kaybolur |
| `JSONL_HEADER_UNEXPECTED`, `JSONL_FOOTER_MISSING`, `STOP_REASON_NOT_DURATION`, `FOOTER_INTERRUPTED_NONZERO` | exclude | dosya eksik ya da run süreyle bitmedi |
| `ACTIVE_STREAMS_NOT_ZERO_BEFORE` | exclude | ölçüm artık akışlarla başladı |
| `ACTIVE_STREAMS_NOT_ZERO_AFTER`, `QUEUE_DEPTH_NOT_ZERO_AFTER` | caution | ölçümden sonraki durum, veri tam |
| `REJECTED_REASON_SHUTDOWN_DELTA_NONZERO` | exclude | ölçüm sırasında shutdown başladı |
| `REJECTED_REASON_<diğer>_DELTA_NONZERO` | caution | sistem davranışı |
| `TIMEOUTS_DELTA_NONZERO`, `RETRIES_DELTA_NONZERO`, `RETRY_EXHAUSTED_DELTA_NONZERO` | caution | fault NONE iken beklenmez, ama ölçülen sistemin davranışıdır |
| `SIMULATOR_ACTIVE_GENERATIONS_NOT_ZERO_AFTER`, `SIMULATOR_REJECTED_DELTA_NONZERO`, `SIMULATOR_STATS_UNAVAILABLE` | caution | ek gözlem; delta'lar n/a olabilir |
| `SIMULATOR_FAULTED_DELTA_NONZERO` | exclude | pinlenmiş iş yükü fault içermez, iş yükü protokolden saptı |
| `GATEWAY_EXITED`, `SIMULATOR_EXITED`, `CONNECTION_MODE_MISMATCH` | exclude | run verisi kullanılamaz |
| `PORT_NOT_FREED` | caution | ölçümden sonra kaydedilir; sonraki run'ın port kontrolü yakalar |
| `CPU_BUSY_BEFORE_RUN` | exclude | runner'ın kendi işaretleyicisi (`cpuBusyThreshold`) ölçüm öncesi arka plan yükü gösterdi |
| `CPU_LOAD_UNAVAILABLE`, `LOG_TRUNCATED`, `LOG_WRITE_ERROR` | caution | veri tam, tanı bilgisi eksik ya da hacim olağandışı |
| `TIME_WAIT_UNAVAILABLE`, `OUTPUT_NOT_GITIGNORED` | info | ortam kaydı |
| `GIT_DIRTY`, `GIT_UNAVAILABLE`, `ARTIFACT_OLDER_THAN_HEAD`, `CHECKLIST_NOT_CONFIRMED`, `CHECKLIST_ANSWER_NO`, `INTERRUPTED` | caution (set) | kimlik banner'ı; kesilen run status'u yüzünden dışlanır |
| `SMOKE_PRESET` | exclude (set) | harness kontrolü, ölçüm değil |
| bilinmeyen bayrak | exclude | analizör anlamını bilmiyor; stdout'ta `unknownFlags` olarak listelenir |

**Analizör kontrolleri** (`CHECK_*` kodları; `Flags.DOCUMENTED` ile kesişmezler):
- exclude: `CHECK_RUN_DIR_INVALID`, `CHECK_FILE_NAME_UNEXPECTED`, `CHECK_MEASURED_JSONL_MISSING`, `CHECK_JSONL_SCHEMA_UNSUPPORTED`, `CHECK_JSONL_MALFORMED_LINE` (ilk 5 satır numarası yazılır, içerik yazılmaz), `CHECK_JSONL_STRUCTURE`, `CHECK_HEADER_PARAMETERS_MISMATCH`, `CHECK_COUNT_MISMATCH`, `CHECK_MODE_UNKNOWN`
- caution: `CHECK_SUMMARY_SCHEMA_UNSUPPORTED`, `CHECK_INTER_CHUNK_UNAVAILABLE` (inter-chunk n/a), `CHECK_SNAPSHOT_UNAVAILABLE`, `CHECK_COUNTER_DECREASED`, `CHECK_REJECTED_METRIC_MISMATCH`, `CHECK_PLAN_MISMATCH`
- info: `CHECK_STOP_METHOD_UNKNOWN`, `CHECK_FIELD_MISSING`

`RunAssessment` en az bir exclude varsa `excluded`, en az bir caution varsa `caution`, yoksa `usable` döner. Gerekçe sırası sınıfa, sonra ada göredir.

### Run başına metrikler (measured faz; `RunMetrics`)

- **Outcome sayıları ve oranları:** request satırları outcome'a göre sayılır. Bilinen sıra loadgen'in `Outcome` sırasıdır (test kapsamı enum'la sabitlenir); bilinmeyen adlar alfabetik sırayla eklenir. Oran = sayı / request satırı sayısı.
- **Tamamlanan akış/s:** `[rampUpMs, durationMs)` penceresinde `rampUp ≤ startOffsetNanos+totalNanos < duration` olan `COMPLETED` kayıtlar sayılır ve `(durationMs − rampUpMs)/1000`'e bölünür. Değerler header'dan alınır ve manifest planıyla karşılaştırılır. `duration ≤ rampUp` ise n/a.
  - Caveat: kapalı modelde ve sabit akış uzunluğunda bu hız büyük ölçüde concurrency, akış uzunluğu ve gateway limitiyle belirlenir; kapasite ölçümü değildir.
  - Bağlam için footer `elapsedNanos` ayrı bir sütunda verilir.
- **ttfb / ttfc / total:** yalnızca `COMPLETED` kayıtlar kullanılır (summary ile aynı popülasyon). Değeri null olan kayıt o dağılıma girmez; "missing" sayısı gösterilir.
  - Kesin nearest-rank kullanılır: sıralı `x[1..n]` için `p = x[ceil(p/100·n)]`, max = `x[n]`. Eşitler doğal olarak ele alınır. `n=0` ise n/a.
  - Yüzdelikler p50, p90, p99, max ve count.
  - Bellek O(COMPLETED) (3 adet `long[]`); hiçbir satır tutulmaz.
- **Inter-chunk:** footer `interChunkHistogram` Base64 olarak çözülür, `Histogram.decodeFromCompressedByteBuffer(buf, 0)` çağrılır.
  - Değerler HdrHistogram'ın `getValueAtPercentile` ve `getMaxValue` sonuçlarıdır (3 anlamlı basamak; kova üst eşdeğeri, yaklaşık %0,1 hassasiyet).
  - Popülasyon run'ın bütün akışlarındaki bütün aralıklardır; COMPLETED dışındaki akışlar da girer.
  - `interChunkCount ≠ totalCount` ise ya da çözüm başarısız olursa `CHECK_INTER_CHUNK_UNAVAILABLE` olur.
- **Generator gecikmesi:** `startOffset − scheduledOffset` için HdrHistogram p99 ve max (yalnızca veri kalitesi göstergesi).
- **Snapshot delta'ları** (`post − pre`, aynı JVM):
  - gateway: `rejected_total` (toplam ve reason başına), `timeouts_total{kind}`, `retries_total{reason}`, `retry_exhausted_total`,
  - simulator: 8 alanın 7 sayacı.
  - Gauge'lar (`active_streams`, `queue_depth`, heap, thread, cpu) delta değil, pre ve post değeri olarak verilir.
  - Azalan sayaç kırpılmaz: `CHECK_COUNTER_DECREASED`.
  - Caveat: pre snapshot warm-up'tan sonra alınır; delta ölçüm fazını ve kısa boşta bekleme sürelerini kapsar.
- **Biçim:**
  - Her şey tam sayı ya da `BigDecimal` ile hesaplanır, `HALF_EVEN` yuvarlanır, `toPlainString()` ile yazılır.
  - Süreler ns'den µs'ye yuvarlanır ve ms cinsinden 3 ondalıkla yazılır.
  - Hız 2 ondalık, oranlar yüzde olarak 2 ondalık, CPU 3 ondalık, bellek GiB olarak 1 ondalık.
  - `String.format` ve varsayılan locale kullanılmaz; büyük/küçük harf dönüşümünde `Locale.ROOT`.

### Karşılaştırma (`CellComparison`, `ComparedMetric`)

- **Metrikler** (sabit enum): `COMPLETED_PER_SECOND, TTFC_P50, TTFC_P99, TOTAL_P50, TOTAL_P99, INTER_CHUNK_P99, REJECTED_SHARE`.
- **Hücre:** concurrency × metrik. Yalnızca `usable` ve `caution` run'lar girer; caution run'lar `*` ile işaretlenir.
- **Tablo:** satırlar repetition'lar. Mod sütunları `modeOrder` sırasıyla; her hücrede değer, `#index` ve pozisyon bulunur. Pozisyon, `(orderLabel=="AB") == (mode==modeOrder[0])` ise `1st`, değilse `2nd`. Tablonun altında mod başına n, medyan (çift n'de iki ortanın ortalaması, kesin) ve min–max verilir.
- Run başına yüzdeliklerin medyanı, birleştirilmiş verinin yüzdeliği değildir; bu açıkça yazılır.
- **Kural** (kesin değerlerle, yuvarlamadan önce uygulanır):
  1. Modlardan birinde `n<2` ise `INSUFFICIENT`.
  2. Medyanlar eşitse `EQUAL`.
  3. Aralıklar örtüşüyorsa (`max(minX,minY) ≤ min(maxX,maxY)`, değmek de örtüşmedir) `CANNOT_SEPARATE_OVERLAP`.
  4. `d=|medX−medY|` için `d ≤ S=max(rangeX, rangeY)` ise `CANNOT_SEPARATE_SPREAD`.
  5. Diğer durumlarda `HIGHER`: bir modun medyanı daha yüksektir.
  - `d` ve `S` yalnızca betimleyicidir. Gerekçe: hücre başına n≤4 kararlı bir varyans tahmini vermez, çıkarım yapılmaz.
- Caution run'ları atılınca karar değişiyorsa ek cümle eklenir (`DEPENDS_ON_CAUTION`).
- **Şablonlar** (`Statement` enum; yer tutucular yalnızca sayı, mod adı, metrik adı ve concurrency):
  - `HIGHER`: "In this result set, at concurrency {c}, the median {metric} of {m1} ({v1}) was higher than that of {m2} ({v2}); the ranges did not overlap and the difference between the medians ({d}) was larger than the largest within-mode range ({s}). With n={n1} and n={n2} repetitions this describes this set only."
  - `CANNOT_SEPARATE_*`: "...the ranges overlapped / ...was not larger than...; this set cannot separate the two modes on this metric."
  - `INSUFFICIENT`: "...fewer than two usable repetitions for {mode} (n={n}); no comparison is stated."

### İfade ve gizlilik korumaları

- **`WordingGuard`** (kelime sınırlı, büyük/küçük harf duyarsız regex). Yasaklı ifadeler:
  `faster, slower, quicker, better, worse, best, worst, outperform*, win, wins, winner, beat, beats, superior, inferior, cheaper, more efficient, less efficient, significant*, p-value, p value, confidence interval, statistically, prove, proves, proven, recommend*, should use, clearly, dramatic*`
  - Muafiyet: `<!-- quoted-limits:begin -->` ile `<!-- quoted-limits:end -->` arası maskelenir. Blok tam olarak bir kez bulunmalı ve dengeli olmalı. Test, içeriğin `docs/benchmark-protocol.md` 2. bölüm maddeleriyle birebir aynı olduğunu doğrular.
  - Kendi düzyazımızda "slower" kullanılmaz ("when responses take longer, the offered load drops").
- **`PrivacyGuard`:** şu desenler aranır: `\b[A-Za-z]:[\\/]`, `\\\\` (UNC), `/home/`, `/Users/`, `/root/`, `/tmp/`, `/mnt/`, `/var/`, `--results` kökünün mutlak yolu, `user.home` değeri. Çıplak `user.name` aranmaz, çünkü CI'da `runner` değeri düzyazıyla çakışır. İhlal exit 1 verir ve hiçbir şey yazılmaz.
- **Kimlik bloğu:**
  - runId, set status'u, preset, createdAt/finishedAt (manifest'ten), toolVersion,
  - git commit, shortCommit, dirty, commitTime, `statusLines` sayısı,
  - os metni, cpu modeli (+kaynağı), logical/physical çekirdek, toplam bellek, Java version/vendor/vmName,
  - güç planı: yalnızca son parantez içindeki ad; null ise "not recorded (not captured on this OS)",
  - checklist cevapları ve kaynağı, `operatorNote` "present / absent", set bayrakları ve sınıfları, `removedEnvironmentVariables` adları.
  - Bulut hesap kimliği kaydedilmiyor. Os metnindeki çekirdek "flavour"u hesap kimliği değildir, olduğu gibi kalır.

### Çıktılar

- **Mekanizma:** `docs/benchmark-report.md` tamamen üretilir; elle düzenlenmez.
  - İnsan yazısı düzyazı, `ReportText` içindeki Java text block'larında durur. Text block'lar satır sonlarını LF'ye normalize ettiği için CRLF checkout sorun çıkarmaz; kod incelemesinden geçer ve taranır.
  - Üretilen tablolar `ReportRenderer` ile eklenir.
  - Marker blokları elle yazılan bir dokümanda reddedildi, çünkü düzyazıdaki sayılar veriden kayabilir.
- **İlk satır:** `<!-- Generated by the report module (analysis version 1) from result set <runId>. Do not edit; regenerate (see "How to regenerate"). -->`
- **Bölüm sırası:**
  1. Identification
  2. What was measured (plan, pinlenmiş gateway özellikleri, loadgen sabit argümanları, JVM seçenekleri)
  3. Run inventory (bütün run'lar + warm-up dosyaları "listed, not analysed")
  4. Per-run metrics
  5. Comparisons per concurrency (metrik tabloları + cümle + grafik bağlantıları)
  6. Gateway and simulator deltas
  7. Data quality
  8. Metric definitions and caveats
  9. Flag classification (tablo `FlagPolicy`'den üretilir)
  10. Limits (alıntı blok)
  11. How to regenerate
- **Sabit dosya listesi:**
  - `<out>/benchmark-report.md`,
  - `<out>/benchmark/runs.csv`,
  - `<out>/benchmark/cells.csv`,
  - `<out>/benchmark/analysis.json`,
  - `<out>/benchmark/chart-completed-per-second.svg`, `chart-ttfc-p50.svg`, `chart-ttfc-p99.svg`, `chart-total-p99.svg`.
  - Adlar veriden bağımsızdır ve her seferinde hepsi yazılır; bu yüzden bayat dosya kalmaz ve hiçbir şey silinmez.
  - Önerilen kullanım `--out docs`: rapor `docs/benchmark-report.md` olur, veri `docs/benchmark/` altına gider.
- **CSV:** RFC 4180, `\n`, sabit sütun sırası. Değerler ham ns ya da kesin ondalık; null boş alan.
- **analysis.json:** `bench.json.Json.write` ile yazılır. Ondalık değerler string, sayaçlar ve ns değerleri Long.
- **SVG** (araç kendisi yazar):
  - 640×360 boyutunda dot plot. x ekseninde concurrency grupları, grup başına mod sütunları; her repetition bir nokta (1st daire, 2nd kare); medyan yatay bir çizgi.
  - y ekseni 0'dan "güzel" bir maksimuma çıkar; tam sayı koordinatlar, `font-family="sans-serif"`, sabit sıra.
  - Dışlanan run'lar çizilmez; başlık bunu söyler.
  - Gerekçe: n=4 için bar ya da hata çubuğu yanıltır; her noktanın görünmesi örtüşmeyi görünür kılar.
- **Commit edilenler:** `docs/benchmark-report.md` (şimdi yer tutucu) ve gerçek analizden sonra `docs/benchmark/*`. Ham set asla commit edilmez. `.gitignore` değişmez.

### CLI (`ReportOptionsParser`; bench stili, `--name value` ve `--name=value`)

| Seçenek | Anlam |
|---|---|
| `--results DIR` | sonuç seti; `--placeholder` ile birlikte kullanılamaz |
| `--out DIR` | var olan bir dizin; `<out>/benchmark/` gerekirse oluşturulur; `--results`'ın içinde olamaz, onu içeremez de |
| `--check` | yalnızca okur ve doğrular, özet yazdırır, hiçbir şey yazmaz; `--out` ile birlikte kullanılamaz |
| `--overwrite` | sabit listedeki dosyaları değiştirmeye izin verir; yalnızca `--out` ile |
| `--placeholder` | yalnızca rapor dosyasını yazar ("No result set analysed yet", sayı yok); `docs/benchmark/` altında veri dosyası varsa exit 2 |
| `--allow-smoke` | smoke set için yazmaya izin verir; bu bayrak olmadan smoke + `--out` exit 2 verir |
| `--help`/`-h` | exit 0 |

- **Stdout (insan okunur):**
  - `report: result set <runId> (status, preset, schema 1, protocol 1)`,
  - status ve sınıfa göre run sayıları,
  - dışlanan her run için tek satır (`id: kaynak:KOD, ...`),
  - warm-up sayısı,
  - yazılan dosyalar (`--out`'a göreli) ya da "check only".
- **Son satır** tek satırlık JSON: `{"type":"report-summary","analysisVersion":1,"runId":..,"runs":..,"usable":..,"caution":..,"excluded":..,"excludedRuns":[{"id":..,"reasons":[..]}],"unknownFlags":[..],"written":[..]}`.
- **Yazma sırası:** önce bütün içerik bellekte üretilir, sonra iki koruma çalışır, ardından dosya başına `.tmp` yazılıp `REPLACE_EXISTING` ile taşınır.
- **Determinizm:** run index sırası; concurrency artan, sonra `modeOrder`; bayraklar sınıf ve ada göre sıralı; `TreeMap`/`LinkedHashMap`; manifest dışında zaman damgası yok; araç sürümü yerine `ANALYSIS_VERSION = 1`.

### Paket `io.github.cemharput.streamlab.report` (main)

`ReportMain`, `ReportOptions`, `ReportOptionsParser`, `UsageException`, `InputException` (exit 2), `ResultSetReader`, `ManifestView` (tipli null-tolerant okuyucular), `SafePaths`, `BoundedLineReader`, `JsonlScanner`, `Percentiles`, `InterChunk`, `SnapshotReader`, `FlagPolicy`, `Checks`, `RunAssessment`, `RunMetrics`, `CellComparison`, `ComparedMetric`, `Statement`, `Fmt`, `ReportText`, `ReportRenderer`, `CsvWriter`, `AnalysisJson`, `SvgDotPlot`, `OutputWriter`, `WordingGuard`, `PrivacyGuard`.

### Runner/manifest bulguları (değiştirilmez, raporlanır)

- **RF1:** schemaVersion 1 sürüm artırılmadan değişti (`stop.method` `graceful`, log anahtarları eksik). Analizör toleranslı, ama alan değişikliği için bir sürüm artırma politikası yok.
- **RF2:** Linux'ta güç ayarının karşılığı (CPU governor), sanallaştırma bilgisi ve ayrılmış çekirdek bilgisi kaydedilmiyor; `powerPlan` her zaman null. Rapor Linux'ta güç ayarı hakkında bir şey söyleyemez.
- **RF3:** ARM `/proc/cpuinfo` dosyasında `model name` yok; model null olur. VM'lerde `physicalCores` vCPU topolojisini yansıtır.
- **RF4:** harness hiç Linux'ta çalıştırılmadı. SIGTERM sonrası exitCode (ör. 143) ve `destroy`'un "hook'lar çalıştı" anlamı doğrulanmadı; analizör bunu bilgi olarak gösterir.
- **RF5:** checklist soruları (`onAcPower`, `samePowerPlan`) masaüstü ya da Windows odaklı; Linux VM'de anlamı belirsiz.
- **RF6:** git klonu dışında (tarball) çalıştırılırsa `GIT_UNAVAILABLE` olur ve commit null kalır; kimlik bloğu eksik kalır. Linux'ta klondan çalıştırılması önerilir.
- **RF7:** `failure.message` ve `harnessFailure` yerel yollar içerebilir; rapor bunları yazmaz.
- **RF8:** smoke dışı bir set Linux'ta hiç üretilmedi; `ss` çıktısı `TIME-WAIT` ile sayılıyor (kod okundu, çalıştırılmadı).

## 3. Uygulama adımları

1. Root `pom.xml` modül satırı ve `report/pom.xml`. `./mvnw -B -pl report -am package -DskipTests` sonrası `report/target/lib` içeriği kontrol edilir: yalnızca bench jar'ı ve HdrHistogram.
2. `ReportOptions`, `ReportOptionsParser` (USAGE metni), `UsageException`, `ReportMain` iskeleti (`run(args, out, err, cwd)` içinde `System.exit` yok) ve `ReportOptionsParserTest`.
3. `testsupport/ResultSetBuilder`:
   - `PlanBuilder.build`, `Manifest.newRoot/newRun` ve `Json` ile manifest kurar.
   - JSONL `Json.writeCompact` ile yazılır; footer histogramı `ConcurrentHistogram(3)` ile kodlanır.
   - Seçenekler: LF/CRLF, Linux tarzı ortam, status, bayraklar, açık ttfc/total değerleri, bozuk satır, sürüm, büyük dosya.
   - Değerler açıkça yapay seçilir (ör. 1..10 ms).
4. `SafePaths`, `BoundedLineReader`, `ManifestView`, `ResultSetReader`, `InputException` ve testleri.
5. `JsonlScanner`, `Percentiles`, `InterChunk`, `SnapshotReader`, `RunMetrics` ve testleri.
6. `FlagPolicy`, `Checks`, `RunAssessment` ve testleri.
7. `CellComparison`, `ComparedMetric`, `Statement` ve testleri.
8. `Fmt`, `ReportText`, `ReportRenderer`, `CsvWriter`, `AnalysisJson`, `SvgDotPlot`, `WordingGuard`, `PrivacyGuard`, `OutputWriter`, yer tutucu ve testleri.
9. `ReportMain` tamamlanır (stdout özeti, exit kodları) ve `ReportMainTest`, `ScenarioTest`, `DeterminismTest`, `PrivacyTest`, `ResultSetUnchangedTest`, `LinuxFlavourTest`, `BoundedMemoryTest`.
10. Sözleşme testleri:
    - `report/src/test/java/io/github/cemharput/streamlab/loadgen/ReportLoadgenFormatContractTest.java`: aynı paket olduğu için package-private `RecordWriter.requestLine/headerLine/footerLine` ve `Summary` erişilebilir. Gerçek formatta satırlar üretilir ve analizör bunları okur; `Outcome.values()` analizörün listesine eşit olmalı. Erişim sorun çıkarırsa reflection'a geçilir.
    - `QuotedLimitsConsistencyTest`, `CommittedReportTest`.
11. Dokümanlar:
    - `docs/adr/0006-benchmark-report.md`,
    - `README.md` (durum, modül satırı, "Benchmark report" bölümü, Decisions),
    - `docs/benchmark-protocol.md`: 1. bölümdeki "Interpreting a result set..." cümlesinin arkasına report modülü ve rapora bağlantı; 9. bölüme "arşivden yeniden üret" komutu; sürüm değişmez.
12. Yer tutucuyu araçla üret: `java -jar report/target/report-0.1.0-SNAPSHOT.jar --placeholder --out docs`.
13. Odaklı testler, 3 kez `verify`, sonra Bölüm 4'teki manuel son adım.

## 4. Doğrulama

| AC | Kontrol | Beklenen |
|---|---|---|
| AC1 | 3× `./mvnw -B verify`; `./mvnw -B -pl report -am dependency:tree`; `report/target/lib` listesi; `git diff --stat -- bench gateway simulator proto loadgen` | yeşil; lib = bench jar + HdrHistogram; diff boş |
| AC2 | `ResultSetReaderTest`, `ReportMainTest` | schema/protocol 2, tool farklı, zorunlu anahtar eksik → exit 2, dosya yok; eski şekiller okunur; `../x`, `C:\x`, `/etc` → `CHECK_RUN_DIR_INVALID`, okunmaz |
| AC3 | `FlagPolicyTest` (her `Flags.DOCUMENTED` girişi ve desen örnekleri, tabloyla birebir; `CHECK_*` ile kesişim yok; bilinmeyen → exclude; warm-up asla exclude değil), `RunAssessmentTest`, `ReportMainTest` (stdout satırları, son satır JSON parse edilir) | sınıflar tabloyla aynı; her run envanterde |
| AC4 | `PercentilesTest` ([5,1,4,2,3] ms → p50 3, p90/p99/max 5; [1,1,1,2] → p50 1, p90 2; 1..100 → p50 50, p90 90, p99 99; n=1; n=0 n/a), `InterChunkTest` (1..10 ns kodlanır → p50 5, p90 9, p99 10, max 10; bu değerlerde ceil ve yuvarlama kuralları aynı sonucu verir; ConcurrentHistogram kodlaması; bozuk Base64 → caution), `JsonlScannerTest` (pencere sınırları dahil/hariç, null ttfc), `RunMetricsTest` (delta, azalan sayaç) | elle hesaplanan değerler |
| AC5 | `CellComparisonTest` | overlap, spread, higher, equal ve insufficient vakaları; 4 rep için pozisyonlar (shared r1 AB → 1st, per-request r1 → 2nd, r2 BA → tersi); ters `modeOrder`; `DEPENDS_ON_CAUTION` |
| AC6 | `StatementWordingTest`, `WordingGuardTest`, bütün senaryo raporlarında tarama | her yasaklı ifade örnek cümlede yakalanır; blok dışına konan yasaklı kelime testi düşürür; tool exit 1 |
| AC7, AC8 | `ReportRendererTest` (bölüm sırası, alıntı blok, caveat ifadeleri, C1–C12 tablosu, GC "listed, not analysed") + küçük golden'lar (text block: envanter, karşılaştırma tablosu + cümle, `runs.csv`, tek SVG) | golden'larla aynı |
| AC9 | `ReportMainTest`, `ResultSetUnchangedTest` | overwrite kuralı, out-in-results → 2, smoke → 2; dosya listesi, boyut, mtime ve sha256 aynı; `--check` yazmaz |
| AC10 | `DeterminismTest` | iki üretim bayt-özdeş; tr-TR/de-DE ve UTC/Asia/Tokyo; karıştırılmış kayıtlar; LF ile CRLF girdileri aynı çıktı |
| AC11 | `PrivacyTest` | md/csv/json/svg/stdout içinde `fixtureuser`, `:\`, `/home/` ve temp kökü yok |
| AC12 | `BoundedMemoryTest`, `BoundedLineReaderTest` | child JVM `-Xmx32m`, yaklaşık 96 MB JSONL (çoğu REJECTED, dolgulu satırlar), exit 0; 5 MiB'lık satır bozuk sayılır |
| AC13 | `LinuxFlavourTest` | aynı metrikler; "not recorded"; `destroy`/143 info; `graceful` → `CHECK_STOP_METHOD_UNKNOWN` |
| AC14 | `ReportMainTest` | 0/1/2 ve help |
| AC15 | inceleme; `ProtocolDocConsistencyTest`; `CommittedReportTest`; `QuotedLimitsConsistencyTest` | yeşil |
| AC16 | kaynakta grep; surefire süreleri | temiz; yaklaşık 30 s altı |

**Yardımcı test notları:**
- `BoundedMemoryTest`'in child classpath'i `ReportMain`, `Json` ve `org.HdrHistogram.Histogram` sınıflarının code-source'larından kurulur. Child çıktısı bir dosyaya yönlendirilir. Bekleme `waitFor(timeout)` ile yapılır, sleep yok.
- `CommittedReportTest` (CRLF normalize edilerek):
  - Committed `docs/benchmark-report.md` dosyasının ilk satırı üretim işaretidir.
  - Dosya yer tutucuysa üretilen yer tutucuyla birebir aynı olmalıdır.
  - Her durumda iki korumadan da geçmelidir; varsa `docs/benchmark/*` de taranır.

**Komutlar** (repo kökünden; PowerShell'de `.\mvnw.cmd`):
- Odaklı: `./mvnw -B -pl report -am verify -Dtest='io/github/cemharput/streamlab/report/**/*Test,ReportLoadgenFormatContractTest' -Dsurefire.failIfNoSpecifiedTests=false`
- `./mvnw -B verify` (3 kez). Windows'ta önceki feature'da geçici `maven-jar-plugin` dosya kilidi görüldü; yeniden deneme raporlanır.
- Build: `./mvnw -B -pl report -am package -DskipTests`
- Analiz: `java -jar report/target/report-0.1.0-SNAPSHOT.jar --results <dir> --check`, ardından `... --results <dir> --out docs --overwrite`
- Yer tutucu: `java -jar report/target/report-0.1.0-SNAPSHOT.jar --placeholder --out docs`

**Manuel son adım (implementer):**
1. Coordinator kullanıcıdan arşiv yolunu ya da run-id'yi alır.
2. Arşiv, repo dışında yeni ve boş bir dizine açılır:
   - Git Bash: `mkdir -p "$HOME/streamlab-sets/x" && tar -xzf <arşiv> -C "$HOME/streamlab-sets/x"`
   - PowerShell: `New-Item -ItemType Directory` ve ardından `tar.exe -xzf <arşiv> -C <dizin>`
   - Alternatif: git'in yok saydığı `bench/results/`.
   - Arşivdeki hiçbir şey çalıştırılmaz.
3. `--check` çalıştırılır: exit kodu ve sınıf sayıları alınır.
4. Kökün altındaki bütün dosyaların sha256 listesi kaydedilir.
5. `--out docs --overwrite` çalıştırılır.
6. Geçici bir dizine ikinci kez üretilir ve bayt-özdeş olduğu karşılaştırılır.
7. Sha256 listesi tekrar alınır; değişmemiş olmalı.
8. `./mvnw -B verify` çalıştırılır.
9. `git status` içinde yalnızca `docs/benchmark-report.md` ve `docs/benchmark/*` görünmeli.
10. Rapora yalnızca şunlar yazılır: üretilen dosya listesi, bölüm başlıkları, run sınıf sayıları ve dışlama kodları. Raporda bulunan sayılar dışında chat'e sayı yazılmaz.

Set yoksa yer tutucu kalır ve "real result set analysis" kalan adım olarak raporlanır; feature bu adım nedeniyle `blocked` olmaz, yer tutucu kabul edilen bir sonuçtur.

**Defekt durumu:** analiz sistematik bir sayım uyuşmazlığı, eksik footer ya da alan kayması gibi runner/loadgen kaynaklı bir sorun gösterirse iş durur. Kanıt (dosya adı, kontrol kodu, satır numarası) raporlanır; o modüller değiştirilmez; run dışlanmış ama görünür kalır.

**Riskler:**
- **n=4 ile yanıltıcı istatistik:** sabit şablonlar, nokta grafikleri, "cannot separate" kuralı ve p-değeri olmaması.
- **Sessiz dışlama:** envanter, stdout ve JSON özeti. Dışlanan run'ların metrikleri de görünür.
- **Ondalık biçimi:** `BigDecimal`, `Locale.ROOT`, locale testleri; `Json`'un `BigDecimal` kesme tuzağına karşı ondalıklar string yazılır.
- **Bellek:** O(COMPLETED) `long`, satır sınırı, child-JVM testi.
- **Bayrak sözlüğü kayması:** `FlagPolicyTest` bench'teki `Flags.DOCUMENTED`'ı kullanır; runner'a yeni bayrak eklenirse report build'i kırılır.
- **Loadgen format kayması:** sözleşme testi ve `Outcome` eşitliği.
- **HdrHistogram 2.2.2 yüzdelik semantiği:** test değerleri iki kuralın aynı sonucu verdiği yerlerden seçilir; implementer kütüphaneyle doğrular.
- **Split package test sınıfı:** classpath'te sorun beklenmez; gerekirse reflection.
- **Exclusion sonrası gRPC'siz classpath'te bench sınıflarının yüklenmesi:** child-JVM testi ve manuel `java -jar` ile doğrulanır.
- **Privacy guard false positive:** düzyazıda Windows yolu kullanılmaz.
- **CI süresi:** büyük dosya testi yaklaşık birkaç saniye.

**Varsayımlar (implementasyonda doğrulanacak):**
- **B1:** `Histogram.decodeFromCompressedByteBuffer`, `ConcurrentHistogram` kodlamasını okur.
- **B2:** bench sınıfları `*:*` exclusion ile çalışır.
- **B3:** loadgen `OptionsParser.parse(String[])` sözleşme testinde dosya oluşturmadan Options üretebilir.
- **B4:** bench'in `PlanBuilder.build(List, int, List)` ve `Manifest.newRun` public API'si fixture için yeterli.
- **B5:** text block golden'ları Windows checkout'ta LF kalır (derleyici normalize eder).

**Açık sorular** (bloklayıcı değil; varsayılan kararlar alındı):
- **S1:** `CPU_BUSY_BEFORE_RUN` exclude olarak bırakıldı (alternatif caution).
- **S2:** bilinmeyen bayrak exclude olarak bırakıldı.
- **S3:** gerçek şekilleri doğrulamak için bir smoke dizininde yalnızca `--check` çalıştırılmasına izin var mı? Varsayılan: hayır (kısıt "smoke verisini kullanma"). Yalnızca sentetik fixture'lar kullanılır.
- **S4:** 4 nokta grafiği eklendi (alternatif yalnızca tablolar).
- **S5:** `operatorNote` rapora konmuyor; makine açıklaması ileride elle yazılacak showcase'e kalır.
