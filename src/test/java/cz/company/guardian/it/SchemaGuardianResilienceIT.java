package cz.company.guardian.it;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static cz.company.guardian.it.ItSupport.TIMEOUT;
import static cz.company.guardian.it.ItSupport.assertRejectedAsInvalidRecord;
import static cz.company.guardian.it.ItSupport.frame;
import static cz.company.guardian.it.ItSupport.proxyWithGuardian;
import static cz.company.guardian.it.ItSupport.storedSchemaIds;
import static cz.company.guardian.it.ItSupport.writeRules;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/** Restart and several replicas against a real broker. */
@ExtendWith(KafkaClusterExtension.class)
@DisabledOnOs(value = OS.WINDOWS, disabledReason = "The in-VM Kafka broker cannot rename log directories on Windows (KAFKA-1194); runs in CI on Linux")
class SchemaGuardianResilienceIT {

    @BrokerCluster
    static KafkaCluster cluster;

    @Test
    void behaviourIsIdenticalAfterTheProxyRestarts(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1, 2 }));

        try (var tester = kroxyliciousTester(proxyWithGuardian(cluster, rules))) {
            try (var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of())) {
                producer.send(new ProducerRecord<>(topic.name(), "k", frame(1, "before"))).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(9, "no"))));
            }

            tester.restartProxy();

            try (var producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of())) {
                producer.send(new ProducerRecord<>(topic.name(), "k", frame(2, "after"))).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(9, "no again"))));
            }
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactly(1, 2);
    }

    @Test
    void everyReplicaEnforcesTheSameWhitelist(Topic topic, @TempDir Path dir) throws Exception {
        Path rules = writeRules(dir, Map.of(topic.name(), new int[]{ 1, 2, 3 }));

        try (var replicaA = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var replicaB = kroxyliciousTester(proxyWithGuardian(cluster, rules));
                var replicaC = kroxyliciousTester(proxyWithGuardian(cluster, rules))) {
            assertThat(replicaA.getBootstrapAddress()).isNotEqualTo(replicaB.getBootstrapAddress());

            int id = 1;
            for (var replica : List.of(replicaA, replicaB, replicaC)) {
                try (var producer = replica.producer(Serdes.String(), Serdes.ByteArray(), Map.of())) {
                    producer.send(new ProducerRecord<>(topic.name(), "k", frame(id++, "ok"))).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                    assertRejectedAsInvalidRecord(producer.send(new ProducerRecord<>(topic.name(), "k", frame(4, "no"))));
                }
            }
        }

        assertThat(storedSchemaIds(cluster, topic.name())).containsExactlyInAnyOrder(1, 2, 3);
    }
}
