package io.github.cemharput.streamlab.report;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Scans generated text for absolute paths and the user's home. The bare user name is not searched (on CI it is a
 * common word); the paths that contain it are.
 */
public final class PrivacyGuard {

    private static final List<Pattern> PATTERNS = List.of(
            Pattern.compile("\\b[A-Za-z]:[\\\\/]"),
            // UNC in its raw, markdown-escaped and JSON-escaped forms: two or more backslashes, server, share
            Pattern.compile("\\\\{2,}[A-Za-z0-9_.$-]+\\\\+[A-Za-z0-9_.$-]+"),
            Pattern.compile("\\\\+\\?\\\\+UNC\\\\+"),
            // UNC after a backslash became a slash (Fmt.code); URLs and ordinary relative paths do not match
            Pattern.compile("(?<![:/\\w])//[A-Za-z0-9_.$-]+/[A-Za-z0-9_.$-]+"),
            Pattern.compile("/home/"),
            Pattern.compile("/Users/"),
            Pattern.compile("/root/"),
            Pattern.compile("/tmp/"),
            Pattern.compile("/mnt/"),
            Pattern.compile("/var/"),
            Pattern.compile("/opt/"),
            Pattern.compile("/srv/"),
            Pattern.compile("/data/"),
            Pattern.compile("/etc/"),
            Pattern.compile("/proc/"),
            Pattern.compile("/sys/"),
            Pattern.compile("/usr/"));

    private PrivacyGuard() {
    }

    /**
     * Returns what was found (a kind, never the text around it); empty means clean. {@code extraSecrets} are literal
     * strings that must not occur, such as the absolute path of the result set and the user's home directory.
     */
    public static List<String> scan(String text, List<String> extraSecrets) {
        List<String> found = new ArrayList<>();
        for (Pattern p : PATTERNS) {
            if (p.matcher(text).find()) {
                found.add("absolute path pattern " + p.pattern());
            }
        }
        for (String secret : extraSecrets) {
            if (secret != null && secret.length() >= 3 && text.contains(secret)) {
                found.add("a path of this machine");
            }
        }
        return found;
    }
}
