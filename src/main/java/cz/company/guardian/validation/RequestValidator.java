package cz.company.guardian.validation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.PartitionProduceData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.TopicProduceData;
import io.kroxylicious.kafka.common.record.internal.BaseRecords;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.MutableRecordBatch;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.utils.BufferSupplier;
import io.kroxylicious.kafka.common.utils.CloseableIterator;

/**
 * Walks every record of a ProduceRequest and applies {@link RecordChecker}. Validation stops at the first violation
 * (fail fast, fail closed). Compressed batches are decompressed by the Kroxylicious record API, there is no way to
 * look inside them otherwise.
 * <p>
 * Instances are immutable and thread-safe.
 */
public final class RequestValidator {

    /**
     * @param checkedRecords number of records that passed before validation finished or stopped
     * @param violation {@code null} when the whole request is allowed
     */
    public record Result(int checkedRecords, Violation violation) {
        public boolean allowed() {
            return violation == null;
        }
    }

    private static final class Scan {
        int checked;
    }

    private final Whitelist whitelist;

    public RequestValidator(Whitelist whitelist) {
        this.whitelist = whitelist;
    }

    /** Topic IDs (Produce v13+) whose names the caller has to resolve before {@link #validate}. */
    public static List<Uuid> unresolvedTopicIds(ProduceRequestData request) {
        List<Uuid> ids = null;
        for (TopicProduceData topic : request.topicData()) {
            if (isBlank(topic.name()) && topic.topicId() != null && !Uuid.ZERO_UUID.equals(topic.topicId())) {
                if (ids == null) {
                    ids = new ArrayList<>(2);
                }
                ids.add(topic.topicId());
            }
        }
        return ids == null ? List.of() : ids;
    }

    /**
     * @param topicNames names for the IDs returned by {@link #unresolvedTopicIds}
     */
    public Result validate(ProduceRequestData request, Map<Uuid, String> topicNames) {
        Scan scan = new Scan();
        boolean anyTopic = false;
        for (TopicProduceData topicData : request.topicData()) {
            anyTopic = true;
            String topic = resolveName(topicData, topicNames);
            if (topic == null) {
                return rejected(scan, Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, null, -1, -1));
            }
            SchemaIdSet allowed = whitelist.allowedFor(topic);
            if (allowed == null) {
                if (whitelist.allowsUnlisted()) {
                    // Not guarded: the records are neither decompressed nor read.
                    continue;
                }
                return rejected(scan, Violation.of(RejectionReason.TOPIC_NOT_CONFIGURED, topic, -1, -1));
            }
            List<PartitionProduceData> partitions = topicData.partitionData();
            if (partitions == null || partitions.isEmpty()) {
                return rejected(scan, Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, topic, -1, -1));
            }
            for (int i = 0; i < partitions.size(); i++) {
                Violation violation = validatePartition(topic, partitions.get(i), allowed, scan);
                if (violation != null) {
                    return rejected(scan, violation);
                }
            }
        }
        if (!anyTopic) {
            return rejected(scan, Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, null, -1, -1));
        }
        return new Result(scan.checked, null);
    }

    private static Result rejected(Scan scan, Violation violation) {
        return new Result(scan.checked, violation);
    }

    private static Violation validatePartition(String topic, PartitionProduceData partition, SchemaIdSet allowed, Scan scan) {
        int partitionIndex = partition.index();
        BaseRecords records = partition.records();
        if (!(records instanceof MemoryRecords memoryRecords)) {
            return Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, topic, partitionIndex, -1);
        }
        int recordIndex = 0;
        boolean sawBatch = false;
        try {
            for (MutableRecordBatch batch : memoryRecords.batches()) {
                sawBatch = true;
                if (batch.isControlBatch()) {
                    // Clients must never write control records; the broker refuses them as well.
                    return Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, topic, partitionIndex, -1);
                }
                try (CloseableIterator<Record> iterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                    while (iterator.hasNext()) {
                        ByteBuffer value = iterator.next().value();
                        RejectionReason reason = RecordChecker.check(value, allowed);
                        if (reason != null) {
                            return new Violation(reason, topic, partitionIndex, recordIndex, RecordChecker.peekSchemaId(value));
                        }
                        recordIndex++;
                        scan.checked++;
                    }
                }
            }
        }
        catch (RuntimeException e) {
            return Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, topic, partitionIndex, recordIndex);
        }
        if (!sawBatch) {
            return Violation.of(RejectionReason.MALFORMED_PRODUCE_REQUEST, topic, partitionIndex, -1);
        }
        return null;
    }

    private static String resolveName(TopicProduceData topicData, Map<Uuid, String> topicNames) {
        String name = topicData.name();
        if (!isBlank(name)) {
            return name;
        }
        Uuid id = topicData.topicId();
        if (id == null || Uuid.ZERO_UUID.equals(id)) {
            return null;
        }
        return topicNames.get(id);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isEmpty();
    }
}
