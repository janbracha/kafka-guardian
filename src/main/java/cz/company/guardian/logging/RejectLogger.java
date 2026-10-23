package cz.company.guardian.logging;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

import cz.company.guardian.validation.TopicNames;
import cz.company.guardian.validation.Violation;

/**
 * Structured, rate limited reject logging. Only the topic, partition, schema ID, reason and connection
 * identifiers are logged: never record keys, values or headers.
 */
public final class RejectLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(RejectLogger.class);

    private final LogRateLimiter limiter;
    private final Logger logger;

    public RejectLogger(LogRateLimiter limiter) {
        this(limiter, LOGGER);
    }

    RejectLogger(LogRateLimiter limiter, Logger logger) {
        this.limiter = limiter;
        this.logger = logger;
    }

    /**
     * @param correlationId Kafka correlation ID of the ProduceRequest
     * @param sessionId opaque, proxy generated identifier of the client connection (no address, no certificate data)
     * @param acksZero whether the producer asked for no response and therefore cannot learn about the rejection
     */
    public void rejected(Violation violation, int correlationId, String sessionId, String virtualCluster, boolean acksZero) {
        if (!logger.isEnabledForLevel(Level.WARN) || !limiter.tryAcquire()) {
            return;
        }
        String topic = TopicNames.sanitize(violation.topic());
        LoggingEventBuilder event = logger.atWarn()
                .addKeyValue("event", "record_rejected")
                .addKeyValue("timestamp", Instant.now().toString())
                .addKeyValue("reason", violation.reason().name())
                .addKeyValue("topic", topic)
                .addKeyValue("partition", violation.partition())
                .addKeyValue("correlationId", correlationId)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("virtualCluster", virtualCluster)
                .addKeyValue("acksZero", acksZero);
        if (violation.hasSchemaId()) {
            event = event.addKeyValue("schemaId", violation.schemaId());
        }
        long suppressed = limiter.drainSuppressed();
        if (suppressed > 0) {
            event = event.addKeyValue("suppressedSincePrevious", suppressed);
        }
        event.log("record_rejected reason={} topic={} partition={}", violation.reason(), topic, violation.partition());
    }

    /** Logs only the exception type and throw site: exception messages might echo request content. */
    public void internalError(Throwable error, int correlationId, String sessionId, String virtualCluster) {
        if (!logger.isEnabledForLevel(Level.ERROR) || !limiter.tryAcquire()) {
            return;
        }
        StackTraceElement[] trace = error.getStackTrace();
        logger.atError()
                .addKeyValue("event", "validation_error")
                .addKeyValue("timestamp", Instant.now().toString())
                .addKeyValue("reason", "INTERNAL_VALIDATION_ERROR")
                .addKeyValue("exception", error.getClass().getName())
                .addKeyValue("location", trace.length > 0 ? trace[0].toString() : "unknown")
                .addKeyValue("correlationId", correlationId)
                .addKeyValue("sessionId", sessionId)
                .addKeyValue("virtualCluster", virtualCluster)
                .log("validation_error exception={}", error.getClass().getName());
    }
}
