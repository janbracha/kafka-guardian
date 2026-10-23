package cz.company.guardian.validation;

/** Why a record (or a whole ProduceRequest) was rejected. The names are used as the Prometheus {@code reason} label. */
public enum RejectionReason {
    TOPIC_NOT_CONFIGURED,
    NULL_VALUE,
    PAYLOAD_TOO_SHORT,
    INVALID_MAGIC_BYTE,
    SCHEMA_NOT_ALLOWED,
    MALFORMED_PRODUCE_REQUEST,
    INTERNAL_VALIDATION_ERROR;

    private static final RejectionReason[] VALUES = values();

    /** Cached {@link #values()} to avoid the defensive array copy on hot paths. */
    public static RejectionReason[] all() {
        return VALUES;
    }
}
