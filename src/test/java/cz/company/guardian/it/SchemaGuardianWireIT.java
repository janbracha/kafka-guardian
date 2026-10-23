package cz.company.guardian.it;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static cz.company.guardian.it.ItSupport.frame;
import static cz.company.guardian.it.ItSupport.proxyWithGuardian;
import static cz.company.guardian.it.ItSupport.storedSchemaIds;
import static cz.company.guardian.it.ItSupport.storedValues;
import static cz.company.guardian.it.ItSupport.writeRules;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Raw ProduceRequests, which {@code KafkaProducer} cannot be coaxed into sending: several topics and partitions in one
 * request, and every Produce version the broker still supports.
 */
@ExtendWith(KafkaClusterExtension.class)
@DisabledOnOs(value = OS.WINDOWS, disabledReason = "The in-VM Kafka broker cannot rename log directories on Windows (KAFKA-1194); runs in CI on Linux")
class SchemaGuardianWireIT {

    @BrokerCluster
    static KafkaCluster cluster;

    private static ProduceRequestData.PartitionProduceData partition(int index, byte[]... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(values[i]);
        }
        return new ProduceRequestData.PartitionProduceData().setIndex(index).setRecords(MemoryRecords.withRecords(Compression.NONE, records));
    }

    private static ProduceRequestData.TopicProduceData byName(String topic, ProduceRequestData.PartitionProduceData... partitions) {
        return new ProduceRequestData.TopicProduceData().setName(topic).setPartitionData(List.of(partitions));
    }

    private static ProduceRequestData.TopicProduceData byId(Uuid topicId, ProduceRequestData.PartitionProduceData... partitions) {
        return new ProduceRequestData.TopicProduceData().setTopicId(topicId).setPartitionData(List.of(partitions));
    }

    private static ProduceRequestData request(short acks, ProduceRequestData.TopicProduceData... topics) {
        var collection = new ProduceRequestData.TopicProduceDataCollection();
        for (var topic : topics) {
            collection.add(topic);
        }
        return new ProduceRequestData().setAcks(acks).setTimeoutMs(10_000).setTopicData(collection);
    }

    private static ProduceResponseData send(KafkaClient client, short version, ProduceRequestData request) {
        Response response = client.getSync(new Request(ApiKeys.PRODUCE, version, "wire-it", request));
        return (ProduceResponseData) response.payload().message();
    }

    private static List<Short> partitionErrors(ProduceResponseData response) {
        return response.responses().stream()
                .flatMap(t -> t.partitionResponses().stream())
                .map(ProduceResponseData.PartitionProduceResponse::errorCode)
                .toList();
    }

    private static Uuid topicId(String topic) throws Exception {
        try (Admin admin = Admin.create(cluster.getKafkaClientConfiguration())) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = { 3, 7, 9, 12, 13 })
    void allowedRequestIsForwardedAndStoredForEveryProduceVersion(short version, Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));
        var data = version >= 13 ? byId(topicId(topic.name()), partition(0, frame(1, "v" + version))) : byName(topic.name(), partition(0, frame(1, "v" + version)));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules)); var client = tester.simpleTestClient()) {
            ProduceResponseData response = send(client, version, request((short) -1, data));

            assertThat(partitionErrors(response)).containsExactly(Errors.NONE.code());
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(1);
    }

    @ParameterizedTest
    @ValueSource(shorts = { 3, 7, 9, 12, 13 })
    void disallowedRequestIsAnsweredWithInvalidRecordForEveryProduceVersion(short version, Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));
        var data = version >= 13 ? byId(topicId(topic.name()), partition(0, frame(2, "no"))) : byName(topic.name(), partition(0, frame(2, "no")));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules)); var client = tester.simpleTestClient()) {
            ProduceResponseData response = send(client, version, request((short) -1, data));

            assertThat(partitionErrors(response)).containsExactly(Errors.INVALID_RECORD.code());
        }

        assertThat(storedValues(cluster, topic.name())).isEmpty();
    }

    @Test
    void oneBadTopicInAMultiTopicMultiPartitionRequestRejectsEverythingAndNothingReachesTheBroker(Topic allowedTopic, Topic otherTopic, @TempDir Path dir)
            throws Exception {
        Path rules = writeRules(dir, Map.of(allowedTopic.name(), new int[]{ 1, 2 }, otherTopic.name(), new int[]{ 7 }));
        var request = request((short) -1,
                byName(allowedTopic.name(), partition(0, frame(1, "a"), frame(2, "b"))),
                byName(otherTopic.name(), partition(0, frame(7, "c"), frame(1, "belongs to the other topic's whitelist"))));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules)); var client = tester.simpleTestClient()) {
            ProduceResponseData response = send(client, (short) 9, request);

            assertThat(partitionErrors(response)).isNotEmpty().allMatch(code -> code == Errors.INVALID_RECORD.code());
        }

        assertThat(storedValues(cluster, allowedTopic.name())).isEmpty();
        assertThat(storedValues(cluster, otherTopic.name())).isEmpty();
    }

    @Test
    void multiTopicRequestWithOnlyAllowedRecordsIsStoredInEveryTopic(Topic first, Topic second, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(first.name(), new int[]{ 1, 2, 3 }, second.name(), new int[]{ 4, 5, 6 }));
        var request = request((short) -1,
                byName(first.name(), partition(0, frame(1, "a"), frame(3, "b"))),
                byName(second.name(), partition(0, frame(4, "c"))));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules)); var client = tester.simpleTestClient()) {
            assertThat(partitionErrors(send(client, (short) 9, request))).containsOnly(Errors.NONE.code());
        }

        assertThat(storedSchemaIds(cluster, first.name())).containsExactly(1, 3);
        assertThat(storedSchemaIds(cluster, second.name())).containsExactly(4);
    }

    @Test
    void connectionRemainsUsableAfterARejection(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules)); var client = tester.simpleTestClient()) {
            assertThat(partitionErrors(send(client, (short) 9, request((short) -1, byName(topic.name(), partition(0, frame(2, "no"))))))).containsExactly(
                    Errors.INVALID_RECORD.code());
            assertThat(partitionErrors(send(client, (short) 9, request((short) -1, byName(topic.name(), partition(0, frame(1, "yes"))))))).containsExactly(
                    Errors.NONE.code());
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(1);
    }
}
