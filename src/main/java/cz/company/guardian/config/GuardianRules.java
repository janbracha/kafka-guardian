package cz.company.guardian.config;

import cz.company.guardian.validation.Whitelist;

/**
 * Validated, immutable contents of a rules file.
 *
 * @param whitelist topic to allowed schema IDs
 * @param includeSchemaIdLabel whether the rejection metric carries a {@code schema_id} label
 * @param maxRejectLogsPerSecond upper bound of individual reject log lines per second
 * @param checksum short SHA-256 of the file contents, exposed as a metric to compare replicas
 */
public record GuardianRules(Whitelist whitelist, boolean includeSchemaIdLabel, int maxRejectLogsPerSecond, String checksum) {

    public static final int DEFAULT_MAX_REJECT_LOGS_PER_SECOND = 5;
}
