package cz.company.guardian.validation;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.compress.Compression;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.PartitionProduceData;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.SimpleRecord;
import io.kroxylicious.testing.filter.record.RecordTestUtils;

import cz.company.guardian.Fixtures;

import static cz.company.guardian.Fixtures.frame;
import static cz.company.guardian.Fixtures.partition;
import static cz.company.guardian.Fixtures.records;
import static cz.company.guardian.Fixtures.request;
import static cz.company.guardian.Fixtures.simpleRequest;
import static cz.company.guardian.Fixtures.topic;
import static org.assertj.core.api.Assertions.assertThat;

class RequestValidatorTest {

    private final RequestValidator validator = new RequestValidator(Fixtures.whitelist());

    private RequestValidator.Result validate(ProduceRequestData request) {
        return validator.validate(request, Map.of());
    }

    @Test
    void allowsRequestWhoseRecordsAllCarryWhitelistedIds() {
        var result = validate(simpleRequest("TEST", frame(1), frame(2), frame(3)));

        assertThat(result.allowed()).isTrue();
        assertThat(result.checkedRecords()).isEqualTo(3);
    }

    @Test
    void rejectsUnconfiguredTopic() {
        var result = validate(simpleRequest("OTHER", frame(1)));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.TOPIC_NOT_CONFIGURED);
        assertThat(result.violation().topic()).isEqualTo("OTHER");
    }

    @Test
    void topicsAreMatchedCaseSensitively() {
        assertThat(validate(simpleRequest("test", frame(1))).violation().reason()).isEqualTo(RejectionReason.TOPIC_NOT_CONFIGURED);
    }

    @Test
    void schemaIdIsCheckedAgainstTheRecordsOwnTopic() {
        assertThat(validate(simpleRequest("TEST", frame(4))).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(validate(simpleRequest("TEST2", frame(4))).allowed()).isTrue();
        assertThat(validate(simpleRequest("TEST2", frame(1))).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void rejectsNullValue() {
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(0L, (byte[]) null, (byte[]) null));

        var result = validate(request(topic("TEST", partition(0, records))));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.NULL_VALUE);
    }

    @Test
    void rejectsPayloadShorterThanFiveBytes() {
        var result = validate(simpleRequest("TEST", new byte[]{ 0, 0, 0, 1 }));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.PAYLOAD_TOO_SHORT);
    }

    @Test
    void rejectsNonConfluentPayload() {
        var result = validate(simpleRequest("TEST", "{\"json\":true}".getBytes()));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.INVALID_MAGIC_BYTE);
    }

    @Test
    void schemaIdZeroAndMaxInt() {
        assertThat(validate(simpleRequest("ZERO", frame(0))).allowed()).isTrue();
        assertThat(validate(simpleRequest("MAXID", frame(Integer.MAX_VALUE))).allowed()).isTrue();
        assertThat(validate(simpleRequest("TEST", frame(0))).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void validRecordsBetweenInvalidOnesDoNotSaveTheRequest() {
        var result = validate(simpleRequest("TEST", frame(9), frame(1), frame(10)));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(result.violation().recordIndex()).isZero();
    }

    @Test
    void invalidRecordBetweenValidOnesRejectsTheRequest() {
        var result = validate(simpleRequest("TEST", frame(1), frame(9), frame(2)));

        assertThat(result.allowed()).isFalse();
        assertThat(result.violation().recordIndex()).isEqualTo(1);
        assertThat(result.violation().schemaId()).isEqualTo(9L);
        assertThat(result.checkedRecords()).isEqualTo(1);
    }

    @Test
    void checksEveryPartitionOfATopic() {
        var allGood = request(topic("TEST", partition(0, records(frame(1))), partition(1, records(frame(2))), partition(2, records(frame(3)))));
        var lastBad = request(topic("TEST", partition(0, records(frame(1))), partition(1, records(frame(2))), partition(2, records(frame(77)))));

        assertThat(validate(allGood).allowed()).isTrue();
        var result = validate(lastBad);
        assertThat(result.violation().partition()).isEqualTo(2);
        assertThat(result.violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void checksEveryTopicOfARequest() {
        var allGood = request(
                topic("TEST", partition(0, records(frame(1)))),
                topic("TEST2", partition(0, records(frame(4)))));
        var secondTopicBad = request(
                topic("TEST", partition(0, records(frame(1)))),
                topic("TEST2", partition(0, records(frame(1)))));

        assertThat(validate(allGood).allowed()).isTrue();
        var result = validate(secondTopicBad);
        assertThat(result.violation().topic()).isEqualTo("TEST2");
        assertThat(result.violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void anUnconfiguredTopicAmongConfiguredOnesRejectsTheRequest() {
        var request = request(
                topic("TEST", partition(0, records(frame(1)))),
                topic("NOPE", partition(0, records(frame(1)))));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.TOPIC_NOT_CONFIGURED);
    }

    @Test
    void checksEveryBatchOfAPartition() {
        MemoryRecords first = records(frame(1));
        MemoryRecords second = records(frame(8));
        ByteBuffer both = ByteBuffer.allocate(first.sizeInBytes() + second.sizeInBytes());
        both.put(first.buffer()).put(second.buffer()).flip();

        var result = validate(request(topic("TEST", partition(0, MemoryRecords.readableRecords(both)))));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(result.violation().recordIndex()).isEqualTo(1);
    }

    static List<Compression> compressions() {
        return List.of(Compression.NONE, Compression.gzip().build(), Compression.lz4().build(), Compression.snappy().build(), Compression.zstd().build());
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void inspectsCompressedBatches(Compression compression) {
        var good = request(topic("TEST", partition(0, records(compression, frame(1), frame(2), frame(3)))));
        var bad = request(topic("TEST", partition(0, records(compression, frame(1), frame(5), frame(3)))));
        var notConfluent = request(topic("TEST", partition(0, records(compression, frame(1), "plain".getBytes()))));

        assertThat(validate(good).allowed()).isTrue();
        assertThat(validate(good).checkedRecords()).isEqualTo(3);
        assertThat(validate(bad).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(validate(bad).violation().recordIndex()).isEqualTo(1);
        assertThat(validate(notConfluent).violation().reason()).isEqualTo(RejectionReason.INVALID_MAGIC_BYTE);
    }

    @Test
    void inspectsIdempotentAndTransactionalBatches() {
        SimpleRecord good = new SimpleRecord(frame(1));
        SimpleRecord bad = new SimpleRecord(frame(99));
        var idempotentOk = MemoryRecords.withIdempotentRecords(Compression.NONE, 4711L, (short) 0, 0, good);
        var transactionalOk = MemoryRecords.withTransactionalRecords(Compression.gzip().build(), 4711L, (short) 0, 0, good);
        var transactionalBad = MemoryRecords.withTransactionalRecords(Compression.NONE, 4711L, (short) 0, 1, good, bad);

        assertThat(validate(request(topic("TEST", partition(0, idempotentOk)))).allowed()).isTrue();
        assertThat(validate(request(topic("TEST", partition(0, transactionalOk)))).allowed()).isTrue();
        assertThat(validate(request(topic("TEST", partition(0, transactionalBad)))).violation().reason())
                .isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void inspectsDirectAndReadOnlyBuffers() {
        MemoryRecords heap = records(frame(2));
        ByteBuffer direct = ByteBuffer.allocateDirect(heap.sizeInBytes());
        direct.put(heap.buffer()).flip();
        MemoryRecords fromDirect = MemoryRecords.readableRecords(direct);
        MemoryRecords fromReadOnly = MemoryRecords.readableRecords(records(frame(2)).buffer().asReadOnlyBuffer());
        MemoryRecords readOnlyBad = MemoryRecords.readableRecords(records(frame(50)).buffer().asReadOnlyBuffer());

        assertThat(validate(request(topic("TEST", partition(0, fromDirect)))).allowed()).isTrue();
        assertThat(validate(request(topic("TEST", partition(0, fromReadOnly)))).allowed()).isTrue();
        assertThat(validate(request(topic("TEST", partition(0, readOnlyBad)))).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
    }

    @Test
    void validationDoesNotConsumeTheRequestBuffers() {
        MemoryRecords records = records(frame(1), frame(2));
        ByteBuffer buffer = records.buffer();
        int position = buffer.position();
        int limit = buffer.limit();
        ProduceRequestData request = request(topic("TEST", partition(0, records)));

        validate(request);
        validate(request);

        assertThat(records.buffer().position()).isEqualTo(position);
        assertThat(records.buffer().limit()).isEqualTo(limit);
        assertThat(validate(request).allowed()).isTrue();
    }

    @Test
    void rejectsPartitionWithoutRecords() {
        var request = request(topic("TEST", new PartitionProduceData().setIndex(0).setRecords(null)));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void rejectsPartitionWithEmptyRecords() {
        var request = request(topic("TEST", partition(0, MemoryRecords.EMPTY)));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void rejectsTopicWithoutPartitions() {
        var request = request(topic("TEST"));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void rejectsRequestWithoutTopics() {
        assertThat(validate(request()).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void rejectsControlBatches() {
        MemoryRecords control = RecordTestUtils.memoryRecords(RecordTestUtils.abortTransactionControlBatch(0));

        var result = validate(request(topic("TEST", partition(0, control))));

        assertThat(result.violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void rejectsCorruptBatchBytes() {
        ByteBuffer corrupt = records(frame(1)).buffer();
        ByteBuffer copy = ByteBuffer.allocate(corrupt.remaining());
        copy.put(corrupt).flip();
        // The record count and length fields live after the 61 byte batch header; destroy them.
        for (int i = 57; i < copy.limit(); i++) {
            copy.put(i, (byte) 0xFF);
        }

        var result = validate(request(topic("TEST", partition(0, MemoryRecords.readableRecords(copy)))));

        assertThat(result.allowed()).isFalse();
        assertThat(result.violation().reason()).isIn(RejectionReason.MALFORMED_PRODUCE_REQUEST, RejectionReason.SCHEMA_NOT_ALLOWED,
                RejectionReason.INVALID_MAGIC_BYTE, RejectionReason.PAYLOAD_TOO_SHORT, RejectionReason.NULL_VALUE);
    }

    @Test
    void resolvesTopicIdsWhenNoNameIsPresent() {
        Uuid id = Uuid.randomUuid();
        var request = request(new ProduceRequestData.TopicProduceData().setTopicId(id).setPartitionData(List.of(partition(0, records(frame(1))))));

        assertThat(RequestValidator.unresolvedTopicIds(request)).containsExactly(id);
        assertThat(validator.validate(request, Map.of(id, "TEST")).allowed()).isTrue();
        assertThat(validator.validate(request, Map.of(id, "TEST2")).violation().reason()).isEqualTo(RejectionReason.SCHEMA_NOT_ALLOWED);
        assertThat(validator.validate(request, Map.of(id, "NOPE")).violation().reason()).isEqualTo(RejectionReason.TOPIC_NOT_CONFIGURED);
    }

    @Test
    void rejectsTopicWithNeitherNameNorKnownId() {
        var request = request(new ProduceRequestData.TopicProduceData().setPartitionData(List.of(partition(0, records(frame(1))))));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
        assertThat(RequestValidator.unresolvedTopicIds(request)).isEmpty();
    }

    @Test
    void topicIdWithoutMappingIsRejected() {
        Uuid id = Uuid.randomUuid();
        var request = request(new ProduceRequestData.TopicProduceData().setTopicId(id).setPartitionData(List.of(partition(0, records(frame(1))))));

        assertThat(validate(request).violation().reason()).isEqualTo(RejectionReason.MALFORMED_PRODUCE_REQUEST);
    }

    @Test
    void aNullPartitionEntryEscapesAsAnExceptionForTheFilterToHandle() {
        var request = request(new ProduceRequestData.TopicProduceData().setName("TEST").setPartitionData(Arrays.asList((PartitionProduceData) null)));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> validate(request)).isInstanceOf(NullPointerException.class);
    }
}
