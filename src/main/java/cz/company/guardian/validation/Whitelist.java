package cz.company.guardian.validation;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Immutable topic to allowed-schema-ID mapping. Safe to share between threads; lookups are O(1) and allocation free. */
public final class Whitelist {

    private final Map<String, SchemaIdSet> topics;

    private Whitelist(Map<String, SchemaIdSet> topics) {
        this.topics = topics;
    }

    public static Whitelist of(Map<String, SchemaIdSet> topics) {
        Objects.requireNonNull(topics, "topics");
        return new Whitelist(Map.copyOf(topics));
    }

    /** @return the allowed IDs, or {@code null} if the topic is not configured (default policy: DENY) */
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
