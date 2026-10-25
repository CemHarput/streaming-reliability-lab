package io.github.cemharput.streamlab.bench;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Git facts of the repository under test. {@code commit} is null when git was unavailable. */
public record GitInfo(String commit, boolean dirty, Instant commitTime, List<String> statusLines,
        boolean ignoredOutput, boolean ignoreKnown) {

    public static final GitInfo UNAVAILABLE = new GitInfo(null, false, null, List.of(), false, false);

    public boolean available() {
        return commit != null;
    }

    public String shortCommit() {
        return commit == null ? null : commit.substring(0, Math.min(7, commit.length()));
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("commit", commit);
        m.put("shortCommit", shortCommit());
        m.put("dirty", available() ? dirty : null);
        m.put("commitTime", commitTime == null ? null : commitTime.toString());
        m.put("statusLines", statusLines);
        return m;
    }
}
