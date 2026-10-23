package cz.company.guardian;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.compress.Compression;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.PartitionProduceData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.TopicProduceData;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.testing.filter.context.MockFilterContext;
import io.kroxylicious.testing.filter.context.MockFilterContext.MockErrorRequestFilterResult;
import io.micrometer.core.instrument.MeterRegistry;

import static cz.company.guardian.Fixtures.frame;
import static cz.company.guardian.Fixtures.header;
import static cz.company.guardian.Fixtures.partition;
import static cz.company.guardian.Fixtures.records;
import static cz.company.guardian.Fixtures.request;
import static cz.company.guardian.Fixtures.simpleRequest;
import static cz.company.guardian.Fixtures.topic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Behaviour of the filter as seen by the proxy: forwarded, or answered locally with INVALID_RECORD. */
class SchemaGuardianFilterTest {

    private final MeterRegistry registry = Fixtures.registry();
    private final SchemaGuardianFilter filter = new SchemaGuardianFilter(Fixtures.runtime(registry));

    private RequestFilterResult run(ProduceRequestData request) {
        return run(request, MockFilterContext.builder(header(), request).build());
    }

    private RequestFilterResult run(ProduceRequestData request, FilterContext context) {
        CompletionStage<RequestFilterResult> stage = filter.onProduceRequest((short) 9, header(), request, context);
        return stage.toCompletableFuture().join();
    }

    private static void assertForwarded(RequestFilterResult result, ProduceRequestData request) {
        assertThat(result).isNotInstanceOf(MockErrorRequestFilterResult.class);
        assertThat(result.shortCircuitResponse()).isFalse();
        assertThat(result.drop()).isFalse();
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.message()).isSameAs(request);
    }

    private static MockErrorRequestFilterResult assertRejected(RequestFilterResult result) {
        assertThat(result).isInstanceOf(MockErrorRequestFilterResult.class);
        MockErrorRequestFilterResult error = (MockErrorRequestFilterResult) result;
        assertThat(error.shortCircuitResponse()).isTrue();
        assertThat(error.error()).isEqualTo(Errors.INVALID_RECORD);
        return error;
    }

    private double count(String name, String tag, String value) {
        return registry.get(name).tag(tag, value).counter().count();
    }

    @Test
    void forwardsRequestWithAllowedSchemaId() {
        ProduceRequestData request = simpleRequest("TEST", frame(1), frame(2));

        assertForwarded(run(request), request);
        assertThat(count("kafka.schema.guardian.produce.requests", "result", "allowed")).isEqualTo(1);
        assertThat(count("kafka.schema.guardian.records", "result", "allowed")).isEqualTo(2);
    }

    @Test
    void rejectsDisallowedSchemaIdWithNonRetriableInvalidRecord() {
        MockErrorRequestFilterResult error = assertRejected(run(simpleRequest("TEST", frame(4))));

        assertThat(error.errorMessage()).contains("SCHEMA_NOT_ALLOWED", "topic=TEST", "partition=0", "recordIndex=0", "schemaId=4");
        assertThat(error.errorMessage()).doesNotContain(Fixtures.PAYLOAD);
    }

    @Test
    void kafkaClientsTreatTheChosenErrorAsNonRetriable() {
        var clientSideError = org.apache.kafka.common.protocol.Errors.forCode(Errors.INVALID_RECORD.code());

        assertThat(clientSideError).isEqualTo(org.apache.kafka.common.protocol.Errors.INVALID_RECORD);
        assertThat(clientSideError.exception()).isInstanceOf(org.apache.kafka.common.InvalidRecordException.class);
        assertThat(clientSideError.exception()).isNotInstanceOf(org.apache.kafka.common.errors.RetriableException.class);
    }

    @Test
    void rejectsUnconfiguredTopic() {
        MockErrorRequestFilterResult error = assertRejected(run(simpleRequest("SECRET", frame(1))));

        assertThat(error.errorMessage()).contains("TOPIC_NOT_CONFIGURED");
    }

    @Test
    void rejectsNullValueShortPayloadAndPlainPayload() {
        assertThat(assertRejected(run(simpleRequest("TEST", (byte[]) null))).errorMessage()).contains("NULL_VALUE");
        assertThat(assertRejected(run(simpleRequest("TEST", new byte[]{ 0, 0, 0 }))).errorMessage()).contains("PAYLOAD_TOO_SHORT");
        assertThat(assertRejected(run(simpleRequest("TEST", "hello world".getBytes()))).errorMessage()).contains("INVALID_MAGIC_BYTE");
    }

    @Test
    void rejectsWholeRequestWhenOneRecordAmongValidOnesIsInvalid() {
        ProduceRequestData request = simpleRequest("TEST", frame(1), frame(2), frame(99), frame(3));

        MockErrorRequestFilterResult error = assertRejected(run(request));

        assertThat(error.errorMessage()).contains("recordIndex=2");
        assertThat(count("kafka.schema.guardian.produce.requests", "result", "rejected")).isEqualTo(1);
        assertThat(count("kafka.schema.guardian.produce.requests", "result", "allowed")).isZero();
    }

    @Test
    void rejectsWholeRequestWhenOnlyTheSecondTopicIsInvalid() {
        ProduceRequestData request = request(
                topic("TEST", partition(0, records(frame(1))), partition(1, records(frame(2)))),
                topic("TEST2", partition(0, records(frame(4))), partition(1, records(frame(1)))));

        MockErrorRequestFilterResult error = assertRejected(run(request));

        assertThat(error.errorMessage()).contains("topic=TEST2", "partition=1");
    }

    @Test
    void forwardsMultiTopicMultiPartitionRequestWhenEverythingIsAllowed() {
        ProduceRequestData request = request(
                topic("TEST", partition(0, records(frame(1))), partition(1, records(frame(3)))),
                topic("TEST2", partition(0, records(frame(4), frame(6)))));

        assertForwarded(run(request), request);
        assertThat(count("kafka.schema.guardian.records", "result", "allowed")).isEqualTo(4);
    }

    @Test
    void worksForCompressedBatches() {
        ProduceRequestData good = request(topic("TEST", partition(0, records(Compression.gzip().build(), frame(1), frame(2)))));
        ProduceRequestData bad = request(topic("TEST", partition(0, records(Compression.gzip().build(), frame(1), frame(8)))));

        assertForwarded(run(good), good);
        assertRejected(run(bad));
    }

    @Test
    void rejectionDoesNotDependOnAcksAndIsReportedForAcksZero() {
        for (short acks : new short[]{ 1, -1 }) {
            ProduceRequestData request = request(acks, topic("TEST", partition(0, records(frame(99)))));
            assertRejected(run(request));
        }
        for (short acks : new short[]{ 0, 1, -1 }) {
            ProduceRequestData allowed = request(acks, topic("TEST", partition(0, records(frame(1)))));
            assertForwarded(run(allowed), allowed);
        }
    }

    @Test
    void acksZeroRejectionDropsTheRequestBecauseTheProtocolHasNoResponseForIt() {
        ProduceRequestData request = request((short) 0, topic("TEST", partition(0, records(frame(99)))));

        RequestFilterResult result = run(request);

        assertThat(result.drop()).isTrue();
        assertThat(result.shortCircuitResponse()).isFalse();
        assertThat(result.message()).isSameAs(request);
        assertThat(registry.get("kafka.schema.guardian.acks0.rejected.requests").counter().count()).isEqualTo(1);
        assertThat(count("kafka.schema.guardian.produce.requests", "result", "rejected")).isEqualTo(1);
    }

    @Test
    void rejectsMalformedRequests() {
        ProduceRequestData nullRecords = request(topic("TEST", new PartitionProduceData().setIndex(0).setRecords(null)));
        ProduceRequestData noTopics = request();

        assertThat(assertRejected(run(nullRecords)).errorMessage()).contains("MALFORMED_PRODUCE_REQUEST");
        assertThat(assertRejected(run(noTopics)).errorMessage()).contains("MALFORMED_PRODUCE_REQUEST");
    }

    @Test
    void exceptionDuringValidationRejectsInsteadOfForwarding() {
        ProduceRequestData request = request(new TopicProduceData().setName("TEST").setPartitionData(Arrays.asList((PartitionProduceData) null)));

        MockErrorRequestFilterResult error = assertRejected(run(request));

        assertThat(error.errorMessage()).contains("INTERNAL_VALIDATION_ERROR");
        assertThat(registry.get("kafka.schema.guardian.internal.errors").counter().count()).isEqualTo(1);
    }

    @Test
    void whenEvenTheRejectionCannotBeBuiltTheStageFailsAndNothingIsForwarded() {
        ProduceRequestData request = request(new TopicProduceData().setName("TEST").setPartitionData(Arrays.asList((PartitionProduceData) null)));
        FilterContext brokenContext = spy(MockFilterContext.builder(header(), request).build());
        doThrow(new IllegalStateException("cannot build results")).when(brokenContext).requestFilterResultBuilder();

        CompletableFuture<RequestFilterResult> stage = filter.onProduceRequest((short) 9, header(), request, brokenContext).toCompletableFuture();

        assertThat(stage).isCompletedExceptionally();
    }

    @Test
    void resolvesTopicIdsForProduceV13() {
        Uuid testId = Uuid.randomUuid();
        Uuid otherId = Uuid.randomUuid();
        ProduceRequestData good = request(new TopicProduceData().setTopicId(testId).setPartitionData(List.of(partition(0, records(frame(1))))));
        ProduceRequestData unconfigured = request(new TopicProduceData().setTopicId(otherId).setPartitionData(List.of(partition(0, records(frame(1))))));
        ProduceRequestData wrongSchema = request(new TopicProduceData().setTopicId(testId).setPartitionData(List.of(partition(0, records(frame(5))))));

        assertForwarded(run(good, context(good, Map.of(testId, "TEST", otherId, "OTHER"))), good);
        assertThat(assertRejected(run(unconfigured, context(unconfigured, Map.of(testId, "TEST", otherId, "OTHER")))).errorMessage())
                .contains("TOPIC_NOT_CONFIGURED");
        assertThat(assertRejected(run(wrongSchema, context(wrongSchema, Map.of(testId, "TEST")))).errorMessage()).contains("SCHEMA_NOT_ALLOWED");
    }

    @Test
    void unresolvableTopicIdIsRejectedNotForwarded() {
        Uuid unknown = Uuid.randomUuid();
        ProduceRequestData request = request(new TopicProduceData().setTopicId(unknown).setPartitionData(List.of(partition(0, records(frame(1))))));

        RequestFilterResult result = run(request, context(request, Map.of()));

        assertThat(result).isInstanceOf(MockErrorRequestFilterResult.class);
        assertThat(((MockErrorRequestFilterResult) result).error()).isEqualTo(Errors.UNKNOWN_TOPIC_ID);
    }

    @Test
    void failingTopicNameLookupIsRejectedNotForwarded() {
        Uuid id = Uuid.randomUuid();
        ProduceRequestData request = request(new TopicProduceData().setTopicId(id).setPartitionData(List.of(partition(0, records(frame(1))))));
        FilterContext failing = spy(MockFilterContext.builder(header(), request).build());
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("metadata unavailable"))).when(failing).topicNames(anyCollection());

        MockErrorRequestFilterResult error = assertRejected(run(request, failing));

        assertThat(error.errorMessage()).contains("INTERNAL_VALIDATION_ERROR");
    }

    @Test
    void doesNotModifyTheRequestItForwards() {
        ProduceRequestData request = simpleRequest("TEST", frame(1));
        ProduceRequestData before = request.duplicate();

        run(request);

        assertThat(request).isEqualTo(before);
    }

    @Test
    void producerFacingMessageNeverContainsRecordContent() {
        MockErrorRequestFilterResult error = assertRejected(run(simpleRequest("TEST", frame(77, Fixtures.PAYLOAD))));

        assertThat(error.errorMessage()).doesNotContain(Fixtures.PAYLOAD).doesNotContain("secret");
    }

    @Test
    void sanitizesUntrustedTopicNameInTheClientMessage() {
        MockErrorRequestFilterResult error = assertRejected(run(simpleRequest("bad\ntopic name", frame(1))));

        assertThat(error.errorMessage()).doesNotContain("\n");
    }

    private static FilterContext context(ProduceRequestData request, Map<Uuid, String> names) {
        return MockFilterContext.builder(header(), request).withTopicNames(names).build();
    }

}
