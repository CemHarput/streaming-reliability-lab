/**
 * Closed-model load generator for the gateway's SSE endpoint. A fixed number of workers each open one stream
 * after the other (linear ramp-up, no think time except the pause after a 503), and every request is written as one raw JSONL record. A
 * convenience summary (counts, HdrHistogram percentiles) is printed, but the JSONL file is the result.
 *
 * <p>Latency is measured from the moment a request is actually sent, so the closed model does not correct for
 * coordinated omission: a slower server lowers the offered load. The open model (fixed arrival rate, intended
 * start time) is deferred; the record already carries {@code scheduledOffsetNanos} for it. See ADR-0004 and the
 * README. No performance claim is made.
 */
package io.github.cemharput.streamlab.loadgen;
