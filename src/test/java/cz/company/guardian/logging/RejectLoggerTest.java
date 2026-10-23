package cz.company.guardian.logging;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import cz.company.guardian.Fixtures;
import cz.company.guardian.validation.RejectionReason;
import cz.company.guardian.validation.Violation;

import static org.assertj.core.api.Assertions.assertThat;

class RejectLoggerTest {

    private static final String LOGGER_NAME = RejectLogger.class.getName();

    private final List<LogEvent> events = new CopyOnWriteArrayList<>();
    private AbstractAppender appender;
    private LoggerContext context;

    @BeforeEach
    void attachAppender() {
        context = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        appender = new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        context.getConfiguration().getLoggerConfig(LOGGER_NAME).addAppender(appender, Level.ALL, null);
        context.updateLoggers();
    }

    @AfterEach
    void detachAppender() {
        context.getConfiguration().getLoggerConfig(LOGGER_NAME).removeAppender("capture");
        context.updateLoggers();
        appender.stop();
    }

    @Test
    void logsOnlyTheDocumentedFields() {
        RejectLogger logger = new RejectLogger(new LogRateLimiter(10));
        Violation violation = new Violation(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 3, 7, 17L);

        logger.rejected(violation, 42, "session-1", "vc", false);

        assertThat(events).hasSize(1);
        Map<String, String> fields = events.get(0).getContextData().toMap();
        assertThat(fields).containsEntry("event", "record_rejected")
                .containsEntry("topic", "TEST")
                .containsEntry("partition", "3")
                .containsEntry("schemaId", "17")
                .containsEntry("reason", "SCHEMA_NOT_ALLOWED")
                .containsEntry("correlationId", "42")
                .containsEntry("sessionId", "session-1")
                .containsKey("timestamp");
        assertThat(fields.keySet()).doesNotContain("key", "value", "headers", "payload");
        assertThat(events.get(0).getMessage().getFormattedMessage()).doesNotContain(Fixtures.PAYLOAD);
    }

    @Test
    void omitsSchemaIdWhenItWasNotReadable() {
        RejectLogger logger = new RejectLogger(new LogRateLimiter(10));

        logger.rejected(Violation.of(RejectionReason.INVALID_MAGIC_BYTE, "TEST", 0, 0), 1, "s", "vc", false);

        assertThat(events.get(0).getContextData().toMap()).doesNotContainKey("schemaId");
    }

    @Test
    void sanitizesClientSuppliedTopicNames() {
        RejectLogger logger = new RejectLogger(new LogRateLimiter(10));

        logger.rejected(Violation.of(RejectionReason.TOPIC_NOT_CONFIGURED, "evil\n{\"forged\":true}", -1, -1), 1, "s", "vc", false);

        assertThat(events.get(0).getContextData().toMap().get("topic")).doesNotContain("\n");
        assertThat(events.get(0).getMessage().getFormattedMessage()).doesNotContain("\n");
    }

    @Test
    void limitsLogVolumeAndReportsSuppressedLines() {
        AtomicLong clock = new AtomicLong(0);
        RejectLogger logger = new RejectLogger(new LogRateLimiter(2, clock::get));
        Violation violation = Violation.of(RejectionReason.SCHEMA_NOT_ALLOWED, "TEST", 0, 0);

        for (int i = 0; i < 10; i++) {
            logger.rejected(violation, i, "s", "vc", false);
        }
        assertThat(events).hasSize(2);

        clock.addAndGet(2_000_000_000L);
        logger.rejected(violation, 99, "s", "vc", false);

        assertThat(events).hasSize(3);
        assertThat(events.get(2).getContextData().toMap()).containsEntry("suppressedSincePrevious", "8");
    }

    @Test
    void internalErrorLogsTypeAndLocationButNotTheMessage() {
        RejectLogger logger = new RejectLogger(new LogRateLimiter(10));

        logger.internalError(new IllegalStateException(Fixtures.PAYLOAD), 5, "s", "vc");

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getContextData().toMap()).containsEntry("exception", "java.lang.IllegalStateException");
        assertThat(events.get(0).toString()).doesNotContain(Fixtures.PAYLOAD);
        assertThat(events.get(0).getThrown()).isNull();
    }
}
