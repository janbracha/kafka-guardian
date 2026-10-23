package cz.company.guardian.it;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import cz.company.guardian.SchemaGuardian;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static org.assertj.core.api.Assertions.assertThat;

/** Shared helpers of the integration tests. They run a real Kroxylicious runtime in front of a real in-VM Kafka broker. */
final class ItSupport {

    static final Duration TIMEOUT = Duration.ofSeconds(30);

    private ItSupport() {
    }

    /** Writes a rules file for the given topics and returns its path. */
    static Path writeRules(Path dir, Map<String, int[]> topics) throws IOException {
        StringBuilder yaml = new StringBuilder("defaultPolicy: DENY\ntopics:\n");
        topics.forEach((topic, ids) -> {
            yaml.append("  ").append(topic).append(":\n    allowedSchemaIds:\n");
            for (int id : ids) {
                yaml.append("      - ").append(id).append('\n');
            }
        });
        Path file = dir.resolve("rules-" + UUID.randomUUID() + ".yaml");
        Files.writeString(file, yaml.toString());
        return file;
    }

    static ConfigurationBuilder proxyWithGuardian(KafkaCluster cluster, Path rulesFile) {
        return proxyWithGuardian(cluster.getBootstrapServers(), rulesFile);
    }

    static ConfigurationBuilder proxyWithGuardian(String upstreamBootstrap, Path rulesFile) {
        var filter = new NamedFilterDefinitionBuilder("guardian", SchemaGuardian.class.getName())
                .withConfig("rulesFile", rulesFile.toString())
                .build();
        var config = proxy(upstreamBootstrap);
        config.addToFilterDefinitions(filter);
        config.addToDefaultFilters(filter.name());
        return config;
    }

    /** Restarting a proxy only works when it listens on a fixed port, so clients can reconnect to the same address. */
    static ConfigurationBuilder proxyWithGuardianOnFixedPort(String upstreamBootstrap, Path rulesFile) {
        int port;
        try (var socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        var filter = new NamedFilterDefinitionBuilder("guardian", SchemaGuardian.class.getName())
                .withConfig("rulesFile", rulesFile.toString())
                .build();
        var upstream = new ClusterDefinition("upstream", upstreamBootstrap, null);
        var virtualCluster = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget("upstream", null))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:" + port).build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(upstream)
                .addToVirtualClusters(virtualCluster)
                .addToFilterDefinitions(filter)
                .addToDefaultFilters(filter.name());
    }

    /** Confluent frame: magic byte 0, big-endian schema id, UTF-8 payload. */
    static byte[] frame(int schemaId, String payload) {
        byte[] body = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(schemaId).put(body).array();
    }

    /** Reads what is really stored in the topic, bypassing the proxy. */
    static List<byte[]> storedValues(KafkaCluster cluster, String topic, boolean readCommitted) {
        Map<String, Object> config = new HashMap<>(cluster.getKafkaClientConfiguration());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (var consumer = new KafkaConsumer<byte[], byte[]>(config)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            List<byte[]> values = new ArrayList<>();
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    values.add(record.value());
                }
            }
            return values;
        }
    }

    static List<byte[]> storedValues(KafkaCluster cluster, String topic) {
        return storedValues(cluster, topic, false);
    }

    static List<Integer> storedSchemaIds(KafkaCluster cluster, String topic) {
        return storedValues(cluster, topic).stream().map(v -> ByteBuffer.wrap(v).getInt(1)).collect(Collectors.toList());
    }

    static double counter(String name, String tagKey, String tagValue) {
        MeterRegistry registry = Metrics.globalRegistry;
        Counter counter = registry.find(name).tag(tagKey, tagValue).counter();
        return counter == null ? 0 : counter.count();
    }

    static double rejectedRequests() {
        return counter("kafka.schema.guardian.produce.requests", "result", "rejected");
    }

    static double allowedRequests() {
        return counter("kafka.schema.guardian.produce.requests", "result", "allowed");
    }

    /** The producer must see the non-retriable INVALID_RECORD error coming from the guardian. */
    static void assertRejectedAsInvalidRecord(Future<RecordMetadata> future) {
        assertThat(future).failsWithin(TIMEOUT)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(InvalidRecordException.class)
                .satisfies(e -> assertThat(e.getCause()).isNotInstanceOf(RetriableException.class))
                .satisfies(e -> assertThat(e.getCause()).hasMessageContaining("kafka-schema-guardian"));
    }
}
