package cz.company.guardian.metrics;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import cz.company.guardian.validation.RejectionReason;
import cz.company.guardian.validation.SchemaIdSet;
import cz.company.guardian.validation.Violation;
import cz.company.guardian.validation.Whitelist;

import static org.assertj.core.api.Assertions.assertThat;

class GuardianMetricsTest {

    private static final Whitelist WHITELIST = Whitelist.of(Map.of("TEST", SchemaIdSet.of(1), "TEST2", SchemaIdSet.of(2)));

    private final MeterRegistry registry = new SimpleMeterRegistry();

    private double rejections(String topic, String reason) {
        return registry.get("kafka.schema.guardian.rejections").tag("topic", topic).tag("reason", reason).counter().count();
    }

    @Test
    void countsAllowedRequestsAndRecords() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, false, "abc");

        metrics.requestAllowed(3, 1_000);
        metrics.requestAllowed(2, 2_000);

        assertThat(registry.get("kafka.schema.guardian.records").tag("result", "allowed").counter().count()).isEqualTo(5);
        assertThat(registry.get("kafka.schema.guardian.produce.requests").tag("result", "allowed").counter().count()).isEqualTo(2);
        assertThat(registry.get("kafka.schema.guardian.validation.duration").timer().count()).isEqualTo(2);
    }

    @Test
    void countsRejectionsPerTopicAndReason() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, false, "abc");

        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 9L), 500, false);
        metrics.requestRejected(Violation.of(RejectionReason.NULL_VALUE, "TEST2", 0, 0), 500, false);

        assertThat(rejections("TEST", "SCHEMA_NOT_ALLOWED")).isEqualTo(1);
        assertThat(rejections("TEST2", "NULL_VALUE")).isEqualTo(1);
        assertThat(rejections("TEST", "NULL_VALUE")).isZero();
        assertThat(registry.get("kafka.schema.guardian.produce.requests").tag("result", "rejected").counter().count()).isEqualTo(1 + 1);
        assertThat(registry.get("kafka.schema.guardian.records").tag("result", "rejected").counter().count()).isEqualTo(2);
    }

    @Test
    void unconfiguredTopicsShareOneBoundedLabelValue() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, false, "abc");

        for (int i = 0; i < 500; i++) {
            metrics.requestRejected(Violation.of(RejectionReason.TOPIC_NOT_CONFIGURED, "random-" + i, -1, -1), 1, false);
        }
        metrics.requestRejected(Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, null, -1, -1), 1, false);

        assertThat(rejections(GuardianMetrics.UNCONFIGURED_TOPIC, "TOPIC_NOT_CONFIGURED")).isEqualTo(500);
        assertThat(rejections(GuardianMetrics.UNCONFIGURED_TOPIC, "MALFORMED_PRODUCE_REQUEST")).isEqualTo(1);
        long series = registry.getMeters().stream().filter(m -> m.getId().getName().equals("kafka.schema.guardian.rejections")).count();
        assertThat(series).isEqualTo(7L * 3);
    }

    @Test
    void schemaIdIsNotALabelByDefault() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, false, "abc");
        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 77L), 1, false);

        assertThat(registry.find("kafka.schema.guardian.rejections").tagKeys("schema_id").meters()).isEmpty();
    }

    @Test
    void schemaIdLabelCanBeEnabledAndIsCapped() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, true, "abc");

        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 77L), 1, false);
        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 77L), 1, false);
        for (int i = 0; i < GuardianMetrics.MAX_SCHEMA_ID_SERIES + 50; i++) {
            metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 1000L + i), 1, false);
        }

        assertThat(registry.get("kafka.schema.guardian.rejections").tag("schema_id", "77").counter().count()).isEqualTo(2);
        assertThat(registry.get("kafka.schema.guardian.rejections").tag("schema_id", "other").counter().count()).isGreaterThan(0);
        long series = registry.find("kafka.schema.guardian.rejections").meters().stream()
                .filter(m -> m.getId().getTag("schema_id") != null && !"none".equals(m.getId().getTag("schema_id"))).count();
        assertThat(series).isLessThanOrEqualTo(GuardianMetrics.MAX_SCHEMA_ID_SERIES + 1);
    }

    @Test
    void countsInternalErrorsAndAcksZeroRejections() {
        GuardianMetrics metrics = new GuardianMetrics(registry, WHITELIST, false, "abc");

        metrics.requestRejected(Violation.of(RejectionReason.INTERNAL_VALIDATION_ERROR, null, -1, -1), 1, true);

        assertThat(registry.get("kafka.schema.guardian.internal.errors").counter().count()).isEqualTo(1);
        assertThat(registry.get("kafka.schema.guardian.acks0.rejected.requests").counter().count()).isEqualTo(1);
    }

    @Test
    void exposesConfiguredTopicCountAndRulesChecksum() {
        new GuardianMetrics(registry, WHITELIST, false, "abc123");

        assertThat(registry.get("kafka.schema.guardian.configured.topics").gauge().value()).isEqualTo(2);
        assertThat(registry.get("kafka.schema.guardian.rules.info").tag("checksum", "abc123").gauge().value()).isEqualTo(1);
    }

    @Test
    void creatingTheMetricsTwiceSharesTheMeters() {
        GuardianMetrics first = new GuardianMetrics(registry, WHITELIST, false, "abc");
        GuardianMetrics second = new GuardianMetrics(registry, WHITELIST, false, "abc");

        first.requestAllowed(1, 1);
        second.requestAllowed(1, 1);

        assertThat(registry.get("kafka.schema.guardian.records").tag("result", "allowed").counter().count()).isEqualTo(2);
    }
}
