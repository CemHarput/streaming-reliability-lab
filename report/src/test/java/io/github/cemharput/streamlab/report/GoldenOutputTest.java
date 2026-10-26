package io.github.cemharput.streamlab.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.report.testsupport.Cli;
import io.github.cemharput.streamlab.report.testsupport.ResultSetBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Small golden outputs of a four-run fixture (two concurrency-2 repetitions of both modes, one caution run): the
 * inventory table, one comparison table with its sentence, runs.csv, cells.csv and one chart. The text blocks are
 * normalised to LF by the compiler, so a CRLF checkout does not matter.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class GoldenOutputTest {

    @TempDir
    Path tmp;

    Path out;
    String report;

    @BeforeEach
    void generate() throws Exception {
        ResultSetBuilder b = ResultSetBuilder.at(tmp.resolve("set")).concurrency(2).repetitions(2).plan();
        b.run(1).completedMs(new long[] {10}, new long[] {110});
        b.run(2).completedMs(new long[] {20}, new long[] {120});
        b.run(3).completedMs(new long[] {22}, new long[] {122}).measuredFlag("OUTCOME_TIMEOUT_NONZERO");
        b.run(4).completedMs(new long[] {12}, new long[] {112});
        Path set = b.build();
        out = Files.createDirectory(tmp.resolve("out"));
        assertThat(Cli.run(tmp, "--results", set.toString(), "--out", out.toString()).exit()).isZero();
        report = Files.readString(out.resolve("benchmark-report.md"));
    }

    @Test
    void inventoryTable() {
        String expected = """
            | # | Run | Mode | Concurrency | Repetition | Order | Position | Status | Class | Exclude and caution reasons | Info |
            |---|---|---|---|---|---|---|---|---|---|---|
            | 1 | 001-c2-shared-r1 | shared | 2 | 1 | AB | 1st | completed | usable | none | none |
            | 2 | 002-c2-per-request-r1 | per-request | 2 | 1 | AB | 2nd | completed | usable | none | none |
            | 3 | 003-c2-per-request-r2 | per-request | 2 | 2 | BA | 1st | completed | caution | `MEASURED:OUTCOME_TIMEOUT_NONZERO` | none |
            | 4 | 004-c2-shared-r2 | shared | 2 | 2 | BA | 2nd | completed | usable | none | none |
            """;
        assertThat(report).contains(expected);
    }

    @Test
    void comparisonTableAndSentence() {
        String expected = """
            #### time to first chunk p50 (ms)

            | Repetition | shared | per-request |
            |---|---|---|
            | 1 | 10.000 #1 1st | 20.000 #2 2nd |
            | 2 | 12.000 #4 2nd | 22.000* #3 1st |

            | Mode | n | Median | Min | Max |
            |---|---|---|---|---|
            | shared | 2 | 11.000 | 10.000 | 12.000 |
            | per-request | 2 | 21.000 | 20.000 | 22.000 |

            In this result set, at concurrency 2, the median time to first chunk p50 of per-request (21.000 ms) was higher than that of shared (11.000 ms); the ranges did not overlap and the difference between the medians (10.000 ms) was larger than the largest within-mode range (2.000 ms). With n=2 and n=2 repetitions this describes this set only.

            This statement depends on the runs marked * (caution). Without them the outcome for this metric would be: fewer than two usable repetitions for one mode.
            """;
        assertThat(report).contains(expected);
    }

    @Test
    void runsCsv() throws Exception {
        String expected = """
            index,id,mode,concurrency,repetition,order_label,position,status,class,reasons,request_lines,completed,rejected,rejected_share_percent,completed_per_second,completed_in_window,window_ms,footer_elapsed_ns,ttfb_n,ttfb_missing,ttfb_p50_ns,ttfb_p90_ns,ttfb_p99_ns,ttfb_max_ns,ttfc_n,ttfc_missing,ttfc_p50_ns,ttfc_p90_ns,ttfc_p99_ns,ttfc_max_ns,total_n,total_missing,total_p50_ns,total_p90_ns,total_p99_ns,total_max_ns,inter_chunk_n,inter_chunk_p50_ns,inter_chunk_p90_ns,inter_chunk_p99_ns,inter_chunk_max_ns,generator_delay_p99_ns,generator_delay_max_ns,outcome_COMPLETED,outcome_ERROR_EVENT,outcome_REJECTED,outcome_HTTP_ERROR,outcome_PROTOCOL_ERROR,outcome_NO_TERMINAL,outcome_RESET,outcome_TIMEOUT,outcome_CONNECT_FAILURE,outcome_INTERRUPTED,outcome_CLIENT_ERROR,outcome_other
            1,001-c2-shared-r1,shared,2,1,AB,1st,completed,usable,,1,1,0,0,0.111111111111111111111111111111,1,9000,10005000000,1,0,1000000,1000000,1000000,1000000,1,0,10000000,10000000,10000000,10000000,1,0,110000000,110000000,110000000,110000000,10,44007423,48005119,49020927,49020927,0,0,1,0,0,0,0,0,0,0,0,0,0,0
            2,002-c2-per-request-r1,per-request,2,1,AB,2nd,completed,usable,,1,1,0,0,0.111111111111111111111111111111,1,9000,10005000000,1,0,1000000,1000000,1000000,1000000,1,0,20000000,20000000,20000000,20000000,1,0,120000000,120000000,120000000,120000000,10,44007423,48005119,49020927,49020927,0,0,1,0,0,0,0,0,0,0,0,0,0,0
            3,003-c2-per-request-r2,per-request,2,2,BA,1st,completed,caution,MEASURED:OUTCOME_TIMEOUT_NONZERO,1,1,0,0,0.111111111111111111111111111111,1,9000,10005000000,1,0,1000000,1000000,1000000,1000000,1,0,22000000,22000000,22000000,22000000,1,0,122000000,122000000,122000000,122000000,10,44007423,48005119,49020927,49020927,0,0,1,0,0,0,0,0,0,0,0,0,0,0
            4,004-c2-shared-r2,shared,2,2,BA,2nd,completed,usable,,1,1,0,0,0.111111111111111111111111111111,1,9000,10005000000,1,0,1000000,1000000,1000000,1000000,1,0,12000000,12000000,12000000,12000000,1,0,112000000,112000000,112000000,112000000,10,44007423,48005119,49020927,49020927,0,0,1,0,0,0,0,0,0,0,0,0,0,0
            """;
        assertThat(Files.readString(out.resolve("benchmark/runs.csv"))).isEqualTo(expected);
    }

    @Test
    void cellsCsv() throws Exception {
        String expected = """
            concurrency,metric,unit,mode,n,median,min,max,statement
            2,completed-per-second,per_second,shared,2,0.111111111111111111111111111111,0.111111111111111111111111111111,0.111111111111111111111111111111,EQUAL
            2,completed-per-second,per_second,per-request,2,0.111111111111111111111111111111,0.111111111111111111111111111111,0.111111111111111111111111111111,EQUAL
            2,ttfc-p50,ns,shared,2,11000000,10000000,12000000,HIGHER
            2,ttfc-p50,ns,per-request,2,21000000,20000000,22000000,HIGHER
            2,ttfc-p99,ns,shared,2,11000000,10000000,12000000,HIGHER
            2,ttfc-p99,ns,per-request,2,21000000,20000000,22000000,HIGHER
            2,total-p50,ns,shared,2,111000000,110000000,112000000,HIGHER
            2,total-p50,ns,per-request,2,121000000,120000000,122000000,HIGHER
            2,total-p99,ns,shared,2,111000000,110000000,112000000,HIGHER
            2,total-p99,ns,per-request,2,121000000,120000000,122000000,HIGHER
            2,inter-chunk-p99,ns,shared,2,49020927,49020927,49020927,EQUAL
            2,inter-chunk-p99,ns,per-request,2,49020927,49020927,49020927,EQUAL
            2,rejected-share,percent,shared,2,0,0,0,EQUAL
            2,rejected-share,percent,per-request,2,0,0,0,EQUAL
            """;
        assertThat(Files.readString(out.resolve("benchmark/cells.csv"))).isEqualTo(expected);
    }

    @Test
    void ttfcP50Chart() throws Exception {
        String expected = """
            <svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360" font-family="sans-serif" font-size="11">
            <rect x="0" y="0" width="640" height="360" fill="#ffffff"/>
            <text x="10" y="18" text-anchor="start" font-size="13">time to first chunk p50 (ms), one point per included run</text>
            <text x="10" y="34" text-anchor="start" font-size="11">Excluded runs are not drawn. Each point is one run; the line is the median of the points in that column.</text>
            <line x1="70" y1="286" x2="620" y2="286" stroke="#d0d0d0"/>
            <text x="64" y="290" text-anchor="end" font-size="10">0.000</text>
            <line x1="70" y1="228" x2="620" y2="228" stroke="#d0d0d0"/>
            <text x="64" y="232" text-anchor="end" font-size="10">12.500</text>
            <line x1="70" y1="171" x2="620" y2="171" stroke="#d0d0d0"/>
            <text x="64" y="175" text-anchor="end" font-size="10">25.000</text>
            <line x1="70" y1="114" x2="620" y2="114" stroke="#d0d0d0"/>
            <text x="64" y="118" text-anchor="end" font-size="10">37.500</text>
            <line x1="70" y1="56" x2="620" y2="56" stroke="#d0d0d0"/>
            <text x="64" y="60" text-anchor="end" font-size="10">50.000</text>
            <line x1="70" y1="56" x2="70" y2="286" stroke="#000000"/>
            <line x1="70" y1="286" x2="620" y2="286" stroke="#000000"/>
            <text x="10" y="50" text-anchor="start" font-size="10">ms</text>
            <circle cx="202" cy="240" r="4" fill="#1f5fa8"/>
            <rect x="208" y="227" width="8" height="8" fill="#1f5fa8"/>
            <line x1="115" y1="235" x2="300" y2="235" stroke="#1f5fa8" stroke-width="2"/>
            <text x="207" y="302" text-anchor="middle" font-size="10">shared</text>
            <rect x="473" y="190" width="8" height="8" fill="#b5541c"/>
            <circle cx="487" cy="185" r="4" fill="#b5541c"/>
            <line x1="390" y1="189" x2="575" y2="189" stroke="#b5541c" stroke-width="2"/>
            <text x="482" y="302" text-anchor="middle" font-size="10">per-request</text>
            <text x="345" y="320" text-anchor="middle" font-size="11">concurrency 2</text>
            <circle cx="76" cy="344" r="4" fill="#555555"/>
            <text x="86" y="348" text-anchor="start" font-size="10">1st run of its pair</text>
            <rect x="196" y="340" width="8" height="8" fill="#555555"/>
            <text x="210" y="348" text-anchor="start" font-size="10">2nd run of its pair</text>
            <text x="330" y="348" text-anchor="start" font-size="10">colours: shared (blue), per-request (orange)</text>
            </svg>
            """;
        assertThat(Files.readString(out.resolve("benchmark/chart-ttfc-p50.svg"))).isEqualTo(expected);
    }
}
