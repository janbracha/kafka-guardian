package cz.company.guardian.validation;

import java.util.regex.Pattern;

/** Topic name helpers. */
public final class TopicNames {

    public static final int MAX_LENGTH = 249;
    private static final Pattern LEGAL = Pattern.compile("[a-zA-Z0-9._-]{1," + MAX_LENGTH + "}");
    private static final String ILLEGAL_PLACEHOLDER = "<illegal-topic-name>";

    private TopicNames() {
    }

    /** Kafka's own topic naming rule. */
    public static boolean isLegal(String name) {
        return name != null && LEGAL.matcher(name).matches() && !name.equals(".") && !name.equals("..");
    }

    /** Client supplied names are untrusted; never let line breaks or control characters reach logs or responses. */
    public static String sanitize(String name) {
        if (name == null) {
            return null;
        }
        return isLegal(name) ? name : ILLEGAL_PLACEHOLDER;
    }
}
