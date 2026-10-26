package cz.company.guardian;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import cz.company.guardian.config.GuardianConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaGuardianTest {

    private final SchemaGuardian factory = new SchemaGuardian();

    private static Path rules(Path dir, String content) throws IOException {
        Path file = dir.resolve("rules.yaml");
        Files.writeString(file, content);
        return file;
    }

    @Test
    void isDiscoverableThroughServiceLoader() {
        boolean found = StreamSupport.stream(ServiceLoader.load(FilterFactory.class).spliterator(), false)
                .anyMatch(SchemaGuardian.class::isInstance);

        assertThat(found).isTrue();
    }

    @Test
    void declaresItsConfigType() {
        assertThat(SchemaGuardian.class.getAnnotation(Plugin.class).configType()).isEqualTo(GuardianConfig.class);
    }

    @Test
    void configIsReadFromTheProxyConfiguration() throws IOException {
        GuardianConfig config = new ObjectMapper().readValue("{\"rulesFile\":\"/rules/rules.yaml\"}", GuardianConfig.class);

        assertThat(config.rulesFile()).isEqualTo("/rules/rules.yaml");
        assertThatThrownBy(() -> new ObjectMapper().readValue("{}", GuardianConfig.class)).isInstanceOf(IOException.class);
    }

    @Test
    void failsStartupWithoutConfiguration() {
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsStartupWithBlankRulesFile() {
        assertThatThrownBy(() -> factory.initialize(null, new GuardianConfig(" "))).isInstanceOf(PluginConfigurationException.class);
        assertThatThrownBy(() -> factory.initialize(null, new GuardianConfig(null))).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsStartupWhenRulesFileIsMissing(@TempDir Path dir) {
        assertThatThrownBy(() -> factory.initialize(null, new GuardianConfig(dir.resolve("nope.yaml").toString())))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsStartupWhenRulesAreInvalid(@TempDir Path dir) throws IOException {
        Path file = rules(dir, "defaultPolicy: PERMIT\ntopics:\n  A:\n    allowedSchemaIds: [1]\n");

        assertThatThrownBy(() -> factory.initialize(null, new GuardianConfig(file.toString())))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("defaultPolicy");
    }

    @Test
    void createsFiltersSharingOneImmutableRuntime(@TempDir Path dir) throws IOException {
        Path file = rules(dir, "defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [1]\n");

        GuardianRuntime runtime = factory.initialize(null, new GuardianConfig(file.toString()));

        assertThat(factory.createFilter(null, runtime)).isInstanceOf(SchemaGuardianFilter.class);
        assertThat(factory.createFilter(null, runtime)).isNotSameAs(factory.createFilter(null, runtime));
    }
}
