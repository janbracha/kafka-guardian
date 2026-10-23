package cz.company.guardian;

import cz.company.guardian.logging.RejectLogger;
import cz.company.guardian.metrics.GuardianMetrics;
import cz.company.guardian.validation.RequestValidator;

/**
 * Immutable, thread-safe state shared by every filter instance (one per client connection) of a virtual cluster.
 */
public record GuardianRuntime(RequestValidator validator, GuardianMetrics metrics, RejectLogger rejectLogger) {
}
