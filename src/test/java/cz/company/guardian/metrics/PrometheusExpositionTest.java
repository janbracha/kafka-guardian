package cz.company.guardian.metrics;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import cz.company.guardian.validation.RejectionReason;
import cz.company.guardian.validation.SchemaIdSet;
import cz.company.guardian.validation.Violation;
import cz.company.guardian.validation.Whitelist;

import static org.assertj.core.api.Assertions.assertThat;

/** The names and labels that end up on the /metrics endpoint of the proxy. */
class PrometheusExpositionTest {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    @Test
    void exposesTheDocumentedSeries() {
        GuardianMetrics metrics = new GuardianMetrics(registry, Whitelist.of(Map.of("TEST", SchemaIdSet.of(1))), false, "c0ffee");
        metrics.requestAllowed(2, 1_000);
        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 5L), 1_000, false);

        String scrape = registry.scrape();

        assertThat(scrape)
                .contains("kafka_schema_guardian_records_total{result=\"allowed\"} 2.0")
                .contains("kafka_schema_guardian_records_total{result=\"rejected\"} 1.0")
                .contains("kafka_schema_guardian_rejections_total{reason=\"SCHEMA_NOT_ALLOWED\",topic=\"TEST\"} 1.0")
                .contains("kafka_schema_guardian_produce_requests_total{result=\"allowed\"} 1.0")
                .contains("kafka_schema_guardian_produce_requests_total{result=\"rejected\"} 1.0")
                .contains("kafka_schema_guardian_internal_errors_total 0.0")
                .contains("kafka_schema_guardian_validation_duration_seconds_count 2")
                .contains("kafka_schema_guardian_configured_topics 1.0")
                .contains("kafka_schema_guardian_rules_info{checksum=\"c0ffee\"} 1")
                .doesNotContain("schema_id=");
    }

    @Test
    void schemaIdLabelAppearsOnlyWhenEnabled() {
        GuardianMetrics metrics = new GuardianMetrics(registry, Whitelist.of(Map.of("TEST", SchemaIdSet.of(1))), true, "c0ffee");
        metrics.requestRejected(new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0, 5L), 1_000, false);

        assertThat(registry.scrape()).contains("schema_id=\"5\"");
    }
}
