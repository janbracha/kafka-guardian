package cz.company.guardian.validation;

/**
 * The first reason found for refusing a ProduceRequest.
 *
 * @param reason why it was refused
 * @param topic topic name, or {@code null} if it could not be determined
 * @param partition partition index, or {@code -1}
 * @param recordIndex index of the offending record within its partition, or {@code -1}
 * @param schemaId schema ID read from the record, or {@link RecordChecker#NO_SCHEMA_ID}
 */
public record Violation(RejectionReason reason, String topic, int partition, int recordIndex, long schemaId) {

    public static Violation of(RejectionReason reason, String topic, int partition, int recordIndex) {
        return new Violation(reason, topic, partition, recordIndex, RecordChecker.NO_SCHEMA_ID);
    }

    public boolean hasSchemaId() {
        return schemaId != RecordChecker.NO_SCHEMA_ID;
    }

    /** Message returned to the producer. Never contains record content. */
    public String clientMessage() {
        StringBuilder sb = new StringBuilder(96)
                .append("kafka-schema-guardian rejected the request: ").append(reason);
        if (topic != null) {
            sb.append(" topic=").append(TopicNames.sanitize(topic));
        }
        if (partition >= 0) {
            sb.append(" partition=").append(partition);
        }
        if (recordIndex >= 0) {
            sb.append(" recordIndex=").append(recordIndex);
        }
        if (hasSchemaId()) {
            sb.append(" schemaId=").append(schemaId);
        }
        return sb.toString();
    }
}
