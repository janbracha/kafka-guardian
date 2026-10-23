package cz.company.guardian.it;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static cz.company.guardian.it.ItSupport.TIMEOUT;
import static cz.company.guardian.it.ItSupport.allowedRequests;
import static cz.company.guardian.it.ItSupport.assertRejectedAsInvalidRecord;
import static cz.company.guardian.it.ItSupport.frame;
import static cz.company.guardian.it.ItSupport.proxyWithGuardian;
import static cz.company.guardian.it.ItSupport.rejectedRequests;
import static cz.company.guardian.it.ItSupport.storedSchemaIds;
import static cz.company.guardian.it.ItSupport.storedValues;
import static cz.company.guardian.it.ItSupport.writeRules;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** KafkaProducer scenarios through a real Kroxylicious runtime in front of a real Kafka broker. */
@ExtendWith(KafkaClusterExtension.class)
@DisabledOnOs(value = OS.WINDOWS, disabledReason = "The in-VM Kafka broker cannot rename log directories on Windows (KAFKA-1194); runs in CI on Linux")
class SchemaGuardianProducerIT {

    @BrokerCluster
    static KafkaCluster cluster;

    private static Map<String, Object> producerConfig(Object... keyValues) {
        Map<String, Object> config = new java.util.HashMap<>();
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        for (int i = 0; i < keyValues.length; i += 2) {
            config.put((String) keyValues[i], keyValues[i + 1]);
        }
        return config;
    }

    @Test
    void recordWithAllowedSchemaIdIsStoredInKafka(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1, 2, 3 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(2, "hello"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(2);
    }

    @Test
    void recordWithDisallowedSchemaIdGetsNonRetriableErrorAndIsNotStored(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1, 2, 3 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            double before = rejectedRequests();

            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(4, "nope"))));

            // A retried request would have been rejected (and counted) again.
            await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> true);
            assertThat(rejectedRequests() - before).isEqualTo(1);
        }

        assertThat(storedValues(cluster, topic.name())).isEmpty();
    }

    @Test
    void newerSchemaVersionOnlyIsAllowedWhenOnlyItsIdIsWhitelisted(Topic topic, @TempDir Path dir) throws Exception {
        // customer-value v1 -> 100, v2 -> 120, v3 -> 150; only 150 is whitelisted
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 150 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(100, "v1"))));
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(120, "v2"))));
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(150, "v3"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(150);
    }

    @Test
    void nonConfluentPayloadIsRejected(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", "{\"json\":true}".getBytes())));
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", new byte[]{ 0, 0, 0 })));
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", (byte[]) null)));
        }

        assertThat(storedValues(cluster, topic.name())).isEmpty();
    }

    @Test
    void topicMissingFromTheWhitelistIsRejected(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of("some-other-topic", new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "x"))));
        }

        assertThat(storedValues(cluster, topic.name())).isEmpty();
    }

    @Test
    void batchContainingOneDisallowedRecordIsRejectedAsAWhole(Topic topic, @TempDir Path dir) {
        Path rules = writeRulesUnchecked(dir, Map.of(topic.name(), new int[]{ 1, 2, 3 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        producerConfig(ProducerConfig.LINGER_MS_CONFIG, 10_000, ProducerConfig.BATCH_SIZE_CONFIG, 1_000_000))) {
            double before = rejectedRequests();
            Future<RecordMetadata> good1 = producer.send(new ProducerRecord<>(topic.name(), 0, "k", frame(1, "a")));
            Future<RecordMetadata> bad = producer.send(new ProducerRecord<>(topic.name(), 0, "k", frame(99, "b")));
            Future<RecordMetadata> good2 = producer.send(new ProducerRecord<>(topic.name(), 0, "k", frame(3, "c")));
            producer.flush();

            assertRejectedAsInvalidRecord(good1);
            assertRejectedAsInvalidRecord(bad);
            assertRejectedAsInvalidRecord(good2);
            assertThat(rejectedRequests() - before).isEqualTo(1);
        }

        assertThat(storedValues(cluster, topic.name())).isEmpty();
    }

    @Test
    void producerKeepsWorkingAfterARejection(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true))) {
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "first"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(2, "rejected"))));
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "second"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(storedValues(cluster, topic.name())).hasSize(2);
        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(1, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "gzip", "snappy", "lz4", "zstd" })
    void compressedBatchesAreInspected(String compression, Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1, 2 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        producerConfig(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression))) {
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(5, "x".repeat(2000)))));
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(2, "y".repeat(2000)))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "1", "all" })
    void rejectionDoesNotDependOnAcks(String acks, Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));
        boolean noResponse = acks.equals("0");

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        producerConfig(ProducerConfig.ACKS_CONFIG, acks, ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, !noResponse && acks.equals("all")))) {
            double before = rejectedRequests();
            Future<RecordMetadata> bad = producer.send(new ProducerRecord<>(topic.name(), "k", frame(9, "bad")));
            if (noResponse) {
                // The protocol has no response for acks=0: the producer cannot learn about the rejection.
                producer.flush();
                await().atMost(TIMEOUT).until(() -> rejectedRequests() - before >= 1);
            }
            else {
                assertRejectedAsInvalidRecord(bad);
            }

            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "good")));
            producer.flush();
            await().atMost(TIMEOUT).until(() -> storedValues(cluster, topic.name()).size() == 1);
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(1);
    }

    @Test
    void transactionalProducerAbortsRejectedTransactionAndCanContinue(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        producerConfig(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + UUID.randomUUID()))) {
            producer.initTransactions();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "in-aborted-tx")));
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(8, "bad"))));
            assertThatThrownBy(producer::commitTransaction).isInstanceOf(KafkaException.class);
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "committed"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
            producer.commitTransaction();
        }

        List<byte[]> committed = storedValues(cluster, topic.name(), true);
        assertThat(committed).hasSize(1);
        assertThat(new String(committed.get(0)).endsWith("committed")).isTrue();
    }

    @Test
    void metricsCountAllowedAndRejectedRequests(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), producerConfig())) {
            double allowedBefore = allowedRequests();
            double rejectedBefore = rejectedRequests();

            producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "ok"))).get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
            assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(2, "no"))));

            assertThat(allowedRequests() - allowedBefore).isEqualTo(1);
            assertThat(rejectedRequests() - rejectedBefore).isEqualTo(1);
        }
    }

    private static Path writeRulesUnchecked(Path dir, Map<String, int[]> topics) {
        try {
            return writeRules(dir, topics);
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
