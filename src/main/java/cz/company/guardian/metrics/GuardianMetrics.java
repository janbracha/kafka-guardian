package cz.company.guardian.metrics;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import cz.company.guardian.validation.RecordChecker;
import cz.company.guardian.validation.RejectionReason;
import cz.company.guardian.validation.Violation;
import cz.company.guardian.validation.Whitelist;

/**
 * Micrometer metrics. Counters for the known topics are created up front so the hot path performs array and
 * hash lookups only. Cardinality is bounded: topics not present in the whitelist share one label value and
 * the optional {@code schema_id} label is capped.
 * <p>
 * Metric failures never influence the validation outcome.
 */
public final class GuardianMetrics {

    public static final String UNCONFIGURED_TOPIC = "__unconfigured__";
    public static final String NO_SCHEMA_ID_LABEL = "none";
    public static final String OTHER_SCHEMA_ID_LABEL = "other";
    public static final int MAX_SCHEMA_ID_SERIES = 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(GuardianMetrics.class);
    private static final String PREFIX = "kafka.schema.guardian.";

    private final MeterRegistry registry;
    private final boolean includeSchemaIdLabel;
    private final Counter recordsAllowed;
    private final Counter recordsRejected;
    private final Counter requestsAllowed;
    private final Counter requestsRejected;
    private final Counter internalErrors;
    private final Counter acksZeroDropped;
    private final Timer validationDuration;
    private final Map<String, Counter[]> rejectionsByTopic;
    private final Counter[] rejectionsUnconfigured;
    private final ConcurrentHashMap<SchemaSeries, Counter> schemaIdSeries = new ConcurrentHashMap<>();

    private record SchemaSeries(String topic, String schemaId) {}

    public GuardianMetrics(MeterRegistry registry, Whitelist whitelist, boolean includeSchemaIdLabel, String rulesChecksum) {
        this.registry = registry;
        this.includeSchemaIdLabel = includeSchemaIdLabel;
        this.recordsAllowed = records("allowed");
        this.recordsRejected = records("rejected");
        this.requestsAllowed = requests("allowed");
        this.requestsRejected = requests("rejected");
        this.internalErrors = Counter.builder(PREFIX + "internal.errors")
                .description("Unexpected exceptions during validation. The affected request was rejected.")
                .register(registry);
        this.acksZeroDropped = Counter.builder(PREFIX + "acks0.rejected.requests")
                .description("Rejected requests sent with acks=0; the producer cannot be told about the rejection.")
                .register(registry);
        this.validationDuration = Timer.builder(PREFIX + "validation.duration")
                .description("Time spent validating one ProduceRequest (excludes topic name resolution).")
                .serviceLevelObjectives(
                        Duration.ofNanos(50_000), Duration.ofNanos(100_000), Duration.ofNanos(250_000), Duration.ofNanos(500_000),
                        Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100))
                .register(registry);

        Map<String, Counter[]> byTopic = new HashMap<>();
        for (String topic : whitelist.topicNames()) {
            byTopic.put(topic, rejectionCounters(topic));
        }
        this.rejectionsByTopic = Map.copyOf(byTopic);
        this.rejectionsUnconfigured = rejectionCounters(UNCONFIGURED_TOPIC);

        Gauge.builder(PREFIX + "configured.topics", whitelist, Whitelist::topicCount)
                .description("Number of topics in the loaded whitelist.")
                .register(registry);
        Gauge.builder(PREFIX + "rules.info", () -> 1)
                .description("Always 1; the checksum label identifies the loaded rules so replicas can be compared.")
                .tag("checksum", rulesChecksum)
                .register(registry);
    }

    private Counter records(String result) {
        return Counter.builder(PREFIX + "records")
                .description("Records seen by the guardian. 'rejected' counts the offending record that stopped validation of a request.")
                .tag("result", result)
                .register(registry);
    }

    private Counter requests(String result) {
        return Counter.builder(PREFIX + "produce.requests")
                .description("ProduceRequests processed by the guardian.")
                .tag("result", result)
                .register(registry);
    }

    private Counter[] rejectionCounters(String topic) {
        RejectionReason[] reasons = RejectionReason.all();
        Counter[] counters = new Counter[reasons.length];
        for (RejectionReason reason : reasons) {
            counters[reason.ordinal()] = rejectionCounter(topic, reason, NO_SCHEMA_ID_LABEL);
        }
        return counters;
    }

    private Counter rejectionCounter(String topic, RejectionReason reason, String schemaIdLabel) {
        Tags tags = Tags.of("topic", topic, "reason", reason.name());
        if (includeSchemaIdLabel) {
            tags = tags.and("schema_id", schemaIdLabel);
        }
        return Counter.builder(PREFIX + "rejections")
                .description("Rejected ProduceRequests by topic and first violation.")
                .tags(tags)
                .register(registry);
    }

    public void requestAllowed(int records, long nanos) {
        try {
            requestsAllowed.increment();
            recordsAllowed.increment(records);
            validationDuration.record(nanos, TimeUnit.NANOSECONDS);
        }
        catch (RuntimeException e) {
            LOGGER.debug("metrics update failed", e);
        }
    }

    public void requestRejected(Violation violation, long nanos, boolean acksZero) {
        try {
            requestsRejected.increment();
            recordsRejected.increment();
            if (acksZero) {
                acksZeroDropped.increment();
            }
            if (violation.reason() == RejectionReason.INTERNAL_VALIDATION_ERROR) {
                internalErrors.increment();
            }
            rejectionCounter(violation).increment();
            validationDuration.record(nanos, TimeUnit.NANOSECONDS);
        }
        catch (RuntimeException e) {
            LOGGER.debug("metrics update failed", e);
        }
    }

    private Counter rejectionCounter(Violation violation) {
        Counter[] byReason = violation.topic() == null ? null : rejectionsByTopic.get(violation.topic());
        if (byReason == null) {
            return rejectionsUnconfigured[violation.reason().ordinal()];
        }
        if (includeSchemaIdLabel && violation.reason() == RejectionReason.SCHEMA_NOT_ALLOWED && violation.schemaId() != RecordChecker.NO_SCHEMA_ID) {
            return schemaIdCounter(violation.topic(), Long.toString(violation.schemaId()));
        }
        return byReason[violation.reason().ordinal()];
    }

    private Counter schemaIdCounter(String topic, String schemaId) {
        SchemaSeries key = new SchemaSeries(topic, schemaId);
        Counter existing = schemaIdSeries.get(key);
        if (existing != null) {
            return existing;
        }
        if (schemaIdSeries.size() >= MAX_SCHEMA_ID_SERIES) {
            return schemaIdSeries.computeIfAbsent(new SchemaSeries(topic, OTHER_SCHEMA_ID_LABEL),
                    k -> rejectionCounter(k.topic(), RejectionReason.SCHEMA_NOT_ALLOWED, OTHER_SCHEMA_ID_LABEL));
        }
        return schemaIdSeries.computeIfAbsent(key, k -> rejectionCounter(k.topic(), RejectionReason.SCHEMA_NOT_ALLOWED, k.schemaId()));
    }
}
