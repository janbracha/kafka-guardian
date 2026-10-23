package cz.company.guardian.it;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.tester.MockServerKroxyliciousTester;

import static cz.company.guardian.it.ItSupport.frame;
import static cz.company.guardian.it.ItSupport.proxyWithGuardian;
import static cz.company.guardian.it.ItSupport.proxyWithGuardianOnFixedPort;
import static cz.company.guardian.it.ItSupport.writeRules;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static io.kroxylicious.testing.integration.tester.MockServerKroxyliciousTester.zeroAckProduceRequestMatcher;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The real Kroxylicious runtime in front of a mock broker. This proves "a rejected ProduceRequest never reaches the
 * broker" by looking at what the broker actually received. It needs no Kafka broker and runs on every OS.
 */
class SchemaGuardianProxyIT {

    private static final String TOPIC = "TEST";
    private static final String TOPIC2 = "TEST2";

    @TempDir
    Path dir;

    private Path rules;

    @BeforeEach
    void writeRulesFile() throws Exception {
        rules = writeRules(dir, Map.of(TOPIC, new int[]{ 1, 2, 3 }, TOPIC2, new int[]{ 4, 5, 6 }));
    }

    private MockServerKroxyliciousTester startProxy(AtomicReference<String> upstream) {
        return startProxy(upstream, false);
    }

    private MockServerKroxyliciousTester startProxy(AtomicReference<String> upstream, boolean fixedPort) {
        MockServerKroxyliciousTester tester = mockKafkaKroxyliciousTester(bootstrap -> {
            upstream.set(bootstrap);
            return fixedPort ? proxyWithGuardianOnFixedPort(bootstrap, rules) : proxyWithGuardian(bootstrap, rules);
        });
        tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.PRODUCE, (short) 9, new ProduceResponseData()));
        return tester;
    }

    private MockServerKroxyliciousTester startProxy() {
        return startProxy(new AtomicReference<>());
    }

    private static ProduceRequestData.PartitionProduceData partition(int index, Compression compression, byte[]... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(values[i]);
        }
        return new ProduceRequestData.PartitionProduceData().setIndex(index).setRecords(MemoryRecords.withRecords(compression, records));
    }

    private static ProduceRequestData.TopicProduceData topic(String name, ProduceRequestData.PartitionProduceData... partitions) {
        return new ProduceRequestData.TopicProduceData().setName(name).setPartitionData(List.of(partitions));
    }

    private static ProduceRequestData request(short acks, ProduceRequestData.TopicProduceData... topics) {
        var collection = new ProduceRequestData.TopicProduceDataCollection();
        for (var topic : topics) {
            collection.add(topic);
        }
        return new ProduceRequestData().setAcks(acks).setTimeoutMs(10_000).setTopicData(collection);
    }

    private static ProduceResponseData produce(KafkaClient client, ProduceRequestData request) {
        var response = client.getSync(new Request(ApiKeys.PRODUCE, (short) 9, "proxy-it", request));
        return (ProduceResponseData) response.payload().message();
    }

    private static List<Short> errors(ProduceResponseData response) {
        return response.responses().stream().flatMap(t -> t.partitionResponses().stream()).map(p -> p.errorCode()).toList();
    }

    private static ProduceRequestData single(String topic, byte[]... values) {
        return request((short) -1, topic(topic, partition(0, Compression.NONE, values)));
    }

    @Test
    void allowedRequestIsForwardedUnchanged() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            ProduceResponseData response = produce(client, single(TOPIC, frame(1, "a"), frame(3, "b")));

            assertThat(errors(response)).isEmpty();
            var forwarded = (ProduceRequestData) tester.getOnlyRequestForApiKey(ApiKeys.PRODUCE).message();
            var records = (MemoryRecords) forwarded.topicData().iterator().next().partitionData().get(0).records();
            var values = new java.util.ArrayList<Integer>();
            records.records().forEach(r -> values.add(ByteBuffer.wrap(ByteBuffer.allocate(r.valueSize()).put(r.value()).array()).getInt(1)));
            assertThat(values).containsExactly(1, 3);
        }
    }

    @Test
    void disallowedRequestNeverReachesTheBrokerAndGetsInvalidRecord() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            ProduceResponseData response = produce(client, single(TOPIC, frame(4, "wrong topic's id")));

            assertThat(errors(response)).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();
        }
    }

    @Test
    void nonConfluentNullAndShortPayloadsNeverReachTheBroker() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            assertThat(errors(produce(client, single(TOPIC, "{\"json\":1}".getBytes())))).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(errors(produce(client, single(TOPIC, new byte[]{ 0, 0, 0, 1 })))).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(errors(produce(client, single(TOPIC, (byte[]) null)))).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(errors(produce(client, single("UNLISTED", frame(1, "x"))))).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "NONE", "GZIP", "SNAPPY", "LZ4", "ZSTD" })
    void compressedBatchWithOneBadRecordNeverReachesTheBroker(String compressionName) {
        Compression compression = Compression.of(compressionName.toLowerCase()).build();
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            var bad = request((short) -1, topic(TOPIC, partition(0, compression, frame(1, "a"), frame(99, "b"), frame(2, "c"))));
            var good = request((short) -1, topic(TOPIC, partition(0, compression, frame(1, "a"), frame(2, "c"))));

            assertThat(errors(produce(client, bad))).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();
            assertThat(errors(produce(client, good))).isEmpty();
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).hasSize(1);
        }
    }

    @Test
    void oneBadPartitionOrTopicRejectsTheWholeRequest() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            var request = request((short) -1,
                    topic(TOPIC, partition(0, Compression.NONE, frame(1, "ok")), partition(1, Compression.NONE, frame(2, "ok"))),
                    topic(TOPIC2, partition(0, Compression.NONE, frame(4, "ok")), partition(1, Compression.NONE, frame(1, "not allowed for TEST2"))));

            ProduceResponseData response = produce(client, request);

            assertThat(errors(response)).hasSize(4).containsOnly(Errors.INVALID_RECORD.code());
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();
        }
    }

    @Test
    void multiTopicRequestWithOnlyAllowedRecordsIsForwardedWhole() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            var request = request((short) -1,
                    topic(TOPIC, partition(0, Compression.NONE, frame(1, "ok")), partition(1, Compression.NONE, frame(2, "ok"))),
                    topic(TOPIC2, partition(0, Compression.NONE, frame(6, "ok"))));

            produce(client, request);

            var forwarded = (ProduceRequestData) tester.getOnlyRequestForApiKey(ApiKeys.PRODUCE).message();
            assertThat(forwarded.topicData()).hasSize(2);
        }
    }

    @Test
    void zeroAcksRejectionIsSilentButNothingIsForwarded() {
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            tester.dropWhen(zeroAckProduceRequestMatcher());

            client.get(new Request(ApiKeys.PRODUCE, (short) 9, "proxy-it", request((short) 0, topic(TOPIC, partition(0, Compression.NONE, frame(99, "bad"))))));
            // The connection must stay usable: a following acknowledged request is answered normally.
            assertThat(errors(produce(client, single(TOPIC, frame(99, "bad again"))))).containsExactly(Errors.INVALID_RECORD.code());

            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();
        }
    }

    @Test
    void producesWithTopicIdsAreResolvedAndChecked() {
        Uuid testId = Uuid.randomUuid();
        try (var tester = startProxy(); var client = tester.simpleTestClient()) {
            var metadata = new MetadataResponseData();
            metadata.topics().add(new MetadataResponseData.MetadataResponseTopic().setName(TOPIC).setTopicId(testId).setErrorCode((short) 0));
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.METADATA, (short) 12, metadata));
            tester.addMockResponseForApiKey(new ResponsePayload(ApiKeys.PRODUCE, (short) 13, new ProduceResponseData()));

            var bad = request((short) -1, new ProduceRequestData.TopicProduceData().setTopicId(testId)
                    .setPartitionData(List.of(partition(0, Compression.NONE, frame(9, "bad")))));
            var good = request((short) -1, new ProduceRequestData.TopicProduceData().setTopicId(testId)
                    .setPartitionData(List.of(partition(0, Compression.NONE, frame(2, "good")))));

            var rejected = (ProduceResponseData) client.getSync(new Request(ApiKeys.PRODUCE, (short) 13, "proxy-it", bad)).payload().message();
            assertThat(errors(rejected)).containsExactly(Errors.INVALID_RECORD.code());
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).isEmpty();

            client.getSync(new Request(ApiKeys.PRODUCE, (short) 13, "proxy-it", good));
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).hasSize(1);
        }
    }

    @Test
    void behaviourIsTheSameAfterARestart() {
        try (var tester = startProxy(new AtomicReference<>(), true)) {
            try (var client = tester.simpleTestClient()) {
                assertThat(errors(produce(client, single(TOPIC, frame(7, "bad"))))).containsExactly(Errors.INVALID_RECORD.code());
            }

            tester.restartProxy();

            try (var client = tester.simpleTestClient()) {
                assertThat(errors(produce(client, single(TOPIC, frame(7, "bad"))))).containsExactly(Errors.INVALID_RECORD.code());
                assertThat(errors(produce(client, single(TOPIC, frame(1, "good"))))).isEmpty();
            }
            assertThat(tester.getRequestsForApiKey(ApiKeys.PRODUCE)).hasSize(1);
        }
    }

    @Test
    void proxyRefusesToStartWhenRulesAreInvalid() throws Exception {
        Path invalid = dir.resolve("invalid.yaml");
        java.nio.file.Files.writeString(invalid, "defaultPolicy: ALLOW\ntopics:\n  A:\n    allowedSchemaIds: [1]\n");

        assertThatThrownBy(() -> kroxyliciousTester(proxyWithGuardian("localhost:9092", invalid)))
                .hasStackTraceContaining("defaultPolicy");
    }

    @Test
    void proxyRefusesToStartWithoutRulesFile() {
        assertThatThrownBy(() -> kroxyliciousTester(proxyWithGuardian("localhost:9092", dir.resolve("missing.yaml"))))
                .hasStackTraceContaining("does not exist");
    }

    @Test
    void severalReplicasEnforceTheSameRules() {
        AtomicReference<String> upstream = new AtomicReference<>();
        try (var first = startProxy(upstream);
                var secondReplica = kroxyliciousTester(proxyWithGuardian(upstream.get(), rules));
                var thirdReplica = kroxyliciousTester(proxyWithGuardian(upstream.get(), rules))) {
            for (var replica : List.of(first.simpleTestClient(), secondReplica.simpleTestClient(), thirdReplica.simpleTestClient())) {
                try (replica) {
                    assertThat(errors(produce(replica, single(TOPIC, frame(4, "bad"))))).containsExactly(Errors.INVALID_RECORD.code());
                    assertThat(errors(produce(replica, single(TOPIC2, frame(4, "good"))))).isEmpty();
                }
            }
            assertThat(first.getRequestsForApiKey(ApiKeys.PRODUCE)).hasSize(3);
        }
    }
}
