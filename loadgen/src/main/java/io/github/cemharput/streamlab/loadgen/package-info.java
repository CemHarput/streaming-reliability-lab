/**
 * Load generator (stage 3). Planned: closed model at fixed concurrency
 * (1/10/50/100) and open model at a fixed arrival rate, with latencies
 * recorded in HdrHistogram from the intended start time so that a saturated
 * generator cannot hide queueing delay.
 */
package io.github.cemharput.streamlab.loadgen;
