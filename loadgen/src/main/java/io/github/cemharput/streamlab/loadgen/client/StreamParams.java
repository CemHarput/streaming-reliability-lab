package io.github.cemharput.streamlab.loadgen.client;

/** Query parameters of one {@code GET /stream} request. All of them are always sent; no gateway default is relied on. */
public record StreamParams(int chunks, int intervalMs, int firstDelayMs, String fault, int faultAtSeq, long seed) {

    public String toQuery() {
        return "chunks=" + chunks
                + "&firstDelayMs=" + firstDelayMs
                + "&intervalMs=" + intervalMs
                + "&fault=" + fault
                + "&faultAtSeq=" + faultAtSeq
                + "&seed=" + seed;
    }
}
