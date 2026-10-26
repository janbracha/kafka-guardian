package cz.company.guardian.validation;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Immutable topic to allowed-schema-ID mapping. Safe to share between threads; lookups are O(1) and allocation free. */
public final class Whitelist {

    private final Map<String, SchemaIdSet> topics;
    private final boolean allowUnlisted;

    private Whitelist(Map<String, SchemaIdSet> topics, boolean allowUnlisted) {
        this.topics = topics;
        this.allowUnlisted = allowUnlisted;
    }

    /** Topics that are not listed are denied. */
    public static Whitelist of(Map<String, SchemaIdSet> topics) {
        return of(topics, false);
    }

    /**
     * @param allowUnlisted {@code true}: topics that are not listed are not inspected at all (default policy ALLOW);
     *            {@code false}: they are rejected (default policy DENY)
     */
    public static Whitelist of(Map<String, SchemaIdSet> topics, boolean allowUnlisted) {
        Objects.requireNonNull(topics, "topics");
        return new Whitelist(Map.copyOf(topics), allowUnlisted);
    }

    public boolean allowsUnlisted() {
        return allowUnlisted;
    }

    /** @return the allowed IDs, or {@code null} if the topic is not listed (then {@link #allowsUnlisted()} decides) */
    public SchemaIdSet allowedFor(String topic) {
        return topic == null ? null : topics.get(topic);
    }

    public int topicCount() {
        return topics.size();
    }

    public Set<String> topicNames() {
        return topics.keySet();
    }

    /** Sorted view for diagnostics and tests. */
    public Map<String, int[]> describe() {
        Map<String, int[]> result = new TreeMap<>();
        topics.forEach((topic, ids) -> result.put(topic, ids.toSortedArray()));
        return result;
    }
}
