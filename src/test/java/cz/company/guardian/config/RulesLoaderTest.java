package cz.company.guardian.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RulesLoaderTest {

    private static final String VALID = """
            defaultPolicy: DENY
            topics:
              TEST:
                allowedSchemaIds:
                  - 1
                  - 2
                  - 3
              TEST2:
                allowedSchemaIds:
                  - 4
                  - 5
                  - 6
            """;

    private static GuardianRules parse(String yaml) {
        return RulesLoader.parse(yaml.getBytes(StandardCharsets.UTF_8), "test");
    }

    private static void assertInvalid(String yaml, String messageFragment) {
        assertThatThrownBy(() -> parse(yaml))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining(messageFragment);
    }

    @Test
    void loadsTheExampleFromTheSpecification() {
        GuardianRules rules = parse(VALID);

        assertThat(rules.whitelist().describe()).containsOnlyKeys("TEST", "TEST2");
        assertThat(rules.whitelist().allowedFor("TEST").toSortedArray()).containsExactly(1, 2, 3);
        assertThat(rules.whitelist().allowedFor("TEST2").toSortedArray()).containsExactly(4, 5, 6);
        assertThat(rules.whitelist().allowedFor("UNKNOWN")).isNull();
        assertThat(rules.includeSchemaIdLabel()).isFalse();
        assertThat(rules.maxRejectLogsPerSecond()).isEqualTo(GuardianRules.DEFAULT_MAX_REJECT_LOGS_PER_SECOND);
        assertThat(rules.checksum()).hasSize(12);
    }

    @Test
    void acceptsJson() {
        GuardianRules rules = parse("{\"defaultPolicy\":\"DENY\",\"topics\":{\"A\":{\"allowedSchemaIds\":[10,20]}}}");

        assertThat(rules.whitelist().allowedFor("A").toSortedArray()).containsExactly(10, 20);
    }

    @Test
    void readsOptionalSettings() {
        GuardianRules rules = parse(VALID + """
                metrics:
                  includeSchemaIdLabel: true
                logging:
                  maxRejectLogsPerSecond: 2
                """);

        assertThat(rules.includeSchemaIdLabel()).isTrue();
        assertThat(rules.maxRejectLogsPerSecond()).isEqualTo(2);
    }

    @Test
    void checksumChangesWithContent() {
        assertThat(parse(VALID).checksum()).isEqualTo(parse(VALID).checksum());
        assertThat(parse(VALID.replace("- 6", "- 7")).checksum()).isNotEqualTo(parse(VALID).checksum());
    }

    @Test
    void acceptsMaximumInt32AndZero() {
        GuardianRules rules = parse("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [0, 2147483647]\n");

        assertThat(rules.whitelist().allowedFor("A").toSortedArray()).containsExactly(0, Integer.MAX_VALUE);
    }

    @Test
    void failsWithoutDefaultPolicy() {
        assertInvalid("topics:\n  A:\n    allowedSchemaIds: [1]\n", "defaultPolicy");
    }

    @ParameterizedTest
    @ValueSource(strings = { "ALLOW", "deny", "Deny", "", "PERMIT" })
    void failsWhenDefaultPolicyIsNotDeny(String policy) {
        assertInvalid("defaultPolicy: \"" + policy + "\"\ntopics:\n  A:\n    allowedSchemaIds: [1]\n", "defaultPolicy");
    }

    @Test
    void failsOnEmptyTopicName() {
        assertInvalid("defaultPolicy: DENY\ntopics:\n  \"\":\n    allowedSchemaIds: [1]\n", "topic name must not be empty");
        assertInvalid("defaultPolicy: DENY\ntopics:\n  \" \":\n    allowedSchemaIds: [1]\n", "topic name must not be empty");
    }

    @Test
    void failsOnIllegalTopicName() {
        assertInvalid("defaultPolicy: DENY\ntopics:\n  \"a b\":\n    allowedSchemaIds: [1]\n", "not a legal Kafka topic name");
        assertInvalid("defaultPolicy: DENY\ntopics:\n  \"..\":\n    allowedSchemaIds: [1]\n", "not a legal Kafka topic name");
    }

    @Test
    void failsWhenTopicHasNoSchemaIds() {
        assertInvalid("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: []\n", "at least one allowedSchemaIds");
        assertInvalid("defaultPolicy: DENY\ntopics:\n  A: {}\n", "at least one allowedSchemaIds");
        assertThatThrownBy(() -> parse("defaultPolicy: DENY\ntopics:\n  A:\n")).isInstanceOf(PluginConfigurationException.class);
        assertThatThrownBy(() -> parse("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds:\n")).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsWhenNoTopicsAreConfigured() {
        assertInvalid("defaultPolicy: DENY\n", "at least one topic");
        assertInvalid("defaultPolicy: DENY\ntopics: {}\n", "at least one topic");
    }

    @Test
    void failsOnNullSchemaId() {
        assertInvalid("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [1, null]\n", "null schema id");
        assertThatThrownBy(() -> parse("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds:\n      - 1\n      -\n"))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsOnNegativeSchemaId() {
        assertInvalid("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [1, -5]\n", "negative schema id: -5");
    }

    @ParameterizedTest
    @ValueSource(strings = { "\"7\"", "1.5", "2147483648", "true", "abc" })
    void failsOnSchemaIdsThatAreNotInt32Numbers(String value) {
        assertThatThrownBy(() -> parse("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [" + value + "]\n"))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsOnInvalidYaml() {
        assertInvalid("defaultPolicy: DENY\ntopics: [unclosed\n", "not valid YAML/JSON");
        assertInvalid("{{{{", "not valid YAML/JSON");
    }

    @Test
    void failsOnEmptyDocument() {
        assertThatThrownBy(() -> parse("")).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsWhenTheSameTopicIsDefinedTwice() {
        assertInvalid("""
                defaultPolicy: DENY
                topics:
                  TEST:
                    allowedSchemaIds: [1]
                  TEST:
                    allowedSchemaIds: [2]
                """, "Duplicate");
    }

    @Test
    void failsWhenDefaultPolicyIsDefinedTwice() {
        assertInvalid("defaultPolicy: DENY\ndefaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [1]\n", "Duplicate");
    }

    @Test
    void failsOnUnknownProperties() {
        assertInvalid(VALID + "extra: true\n", "extra");
        assertInvalid("defaultPolicy: DENY\ntopics:\n  A:\n    allowedSchemaIds: [1]\n    allowedSchemaId: [2]\n", "allowedSchemaId");
    }

    @Test
    void failsOnSecondYamlDocument() {
        assertThatThrownBy(() -> parse(VALID + "---\ndefaultPolicy: DENY\ntopics:\n  OTHER:\n    allowedSchemaIds: [9]\n"))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsOnInvalidLogSettings() {
        assertInvalid(VALID + "logging:\n  maxRejectLogsPerSecond: -1\n", "maxRejectLogsPerSecond");
    }

    @Test
    void reportsAllProblemsAtOnce() {
        assertThatThrownBy(() -> parse("defaultPolicy: ALLOW\ntopics:\n  A:\n    allowedSchemaIds: [-1]\n"))
                .hasMessageContaining("defaultPolicy")
                .hasMessageContaining("negative schema id");
    }

    @Test
    void loadsFromFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("rules.yaml");
        Files.writeString(file, VALID);

        assertThat(RulesLoader.load(file).whitelist().topicCount()).isEqualTo(2);
    }

    @Test
    void failsWhenFileIsMissing(@TempDir Path dir) {
        assertThatThrownBy(() -> RulesLoader.load(dir.resolve("missing.yaml")))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void failsWhenFileIsTooLarge(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("huge.yaml");
        Files.write(file, new byte[(int) RulesLoader.MAX_FILE_BYTES + 1]);

        assertThatThrownBy(() -> RulesLoader.load(file))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("larger than");
    }

    @Test
    void failsWhenPathIsADirectory(@TempDir Path dir) {
        assertThatThrownBy(() -> RulesLoader.load(dir)).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void failsWhenPathIsNull() {
        assertThatThrownBy(() -> RulesLoader.load(null)).isInstanceOf(PluginConfigurationException.class);
    }
}
