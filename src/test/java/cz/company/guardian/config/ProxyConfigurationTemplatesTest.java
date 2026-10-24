package cz.company.guardian.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/** The shipped proxy configurations must be accepted by the real Kroxylicious configuration parser. */
class ProxyConfigurationTemplatesTest {

    static Stream<Path> proxyConfigs() {
        return Stream.of(
                Path.of("deploy/base/proxy-config.yaml"),
                Path.of("deploy/overlays/tls/proxy-config.yaml"),
                Path.of("deploy/overlays/plaintext/proxy-config.yaml"),
                Path.of("examples/local/proxy-config.yaml"));
    }

    @ParameterizedTest
    @MethodSource("proxyConfigs")
    void parsesAndPutsTheGuardianFirst(Path file) throws IOException {
        Configuration configuration = new ConfigParser().parseConfiguration(Files.readString(file));

        assertThat(configuration.defaultFilters()).first().isEqualTo("schema-guardian");
        assertThat(configuration.filterDefinitions()).anySatisfy(f -> {
            assertThat(f.name()).isEqualTo("schema-guardian");
            assertThat(f.type()).isEqualTo("SchemaGuardian");
            assertThat(f.config()).isInstanceOf(GuardianConfig.class);
        });
        assertThat(configuration.virtualClusters()).hasSize(1);
        assertThat(configuration.virtualClusters().get(0).gateways()).hasSize(1);
    }

    @Test
    void shippedRulesAreValid() {
        GuardianRules rules = RulesLoader.load(Path.of("deploy/base/rules.yaml"));

        assertThat(rules.whitelist().describe()).containsOnlyKeys("TEST", "TEST2");
        assertThat(RulesLoader.load(Path.of("docker/rules.yaml")).whitelist().topicCount()).isPositive();
        assertThat(RulesLoader.load(Path.of("examples/local/rules.yaml")).whitelist().topicNames()).containsExactly("orders");
    }

    @Test
    void servicePortsMatchTheGatewayPorts() throws IOException {
        String service = Files.readString(Path.of("deploy/base/service.yaml"));
        String deployment = Files.readString(Path.of("deploy/base/deployment.yaml"));

        for (int port : new int[]{ 9092, 9093, 9094, 9095, 9190 }) {
            assertThat(service).contains("port: " + port);
            assertThat(deployment).contains("containerPort: " + port);
        }
    }
}
