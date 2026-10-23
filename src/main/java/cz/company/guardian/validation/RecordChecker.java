package cz.company.guardian.validation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Stateless check of a single record value against the Confluent wire format header:
 * {@code magic byte (0) | schema id (4 bytes, big-endian) | payload}. The payload is never read.
 * <p>
 * Only absolute reads are used, so position, limit and mark of the supplied buffer are never modified.
 * No allocation takes place.
 */
public final class RecordChecker {

    public static final int HEADER_LENGTH = 5;
    public static final byte MAGIC_BYTE = 0;

    private RecordChecker() {
    }

    /**
     * @param value the record value, may be {@code null} (tombstone)
     * @param allowed whitelist for the record's topic
     * @return {@code null} if the record is allowed, otherwise the reason for rejection
     */
    public static RejectionReason check(ByteBuffer value, SchemaIdSet allowed) {
        if (value == null) {
            return RejectionReason.NULL_VALUE;
        }
        int position = value.position();
        if (value.limit() - position < HEADER_LENGTH) {
            return RejectionReason.PAYLOAD_TOO_SHORT;
        }
        if (value.get(position) != MAGIC_BYTE) {
            return RejectionReason.INVALID_MAGIC_BYTE;
        }
        return allowed.contains(readSchemaId(value, position + 1)) ? null : RejectionReason.SCHEMA_NOT_ALLOWED;
    }

    /**
     * Reads the schema ID of a value for diagnostics.
     *
     * @return the schema ID, or {@link #NO_SCHEMA_ID} if the value is not a well-formed Confluent frame
     */
    public static long peekSchemaId(ByteBuffer value) {
        if (value == null) {
            return NO_SCHEMA_ID;
        }
        int position = value.position();
        if (value.limit() - position < HEADER_LENGTH || value.get(position) != MAGIC_BYTE) {
            return NO_SCHEMA_ID;
        }
        return readSchemaId(value, position + 1);
    }

    /** Sentinel for "no schema ID could be read". Outside the {@code int} range, so it cannot clash with a real ID. */
    public static final long NO_SCHEMA_ID = Long.MIN_VALUE;

    private static int readSchemaId(ByteBuffer value, int index) {
        if (value.order() == ByteOrder.BIG_ENDIAN) {
            return value.getInt(index);
        }
        return ((value.get(index) & 0xFF) << 24)
                | ((value.get(index + 1) & 0xFF) << 16)
                | ((value.get(index + 2) & 0xFF) << 8)
                | (value.get(index + 3) & 0xFF);
    }
}
