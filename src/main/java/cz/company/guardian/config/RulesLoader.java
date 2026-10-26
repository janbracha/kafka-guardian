package cz.company.guardian.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import cz.company.guardian.validation.SchemaIdSet;
import cz.company.guardian.validation.TopicNames;
import cz.company.guardian.validation.Whitelist;

/**
 * Reads and strictly validates a rules file (YAML or JSON). Any problem results in a {@link PluginConfigurationException},
 * which makes the proxy refuse to start (fail closed).
 */
public final class RulesLoader {

    static final long MAX_FILE_BYTES = 1024 * 1024;
    static final int MAX_LOGS_PER_SECOND_LIMIT = 100_000;

    private static final ObjectMapper MAPPER = new ObjectMapper(YAMLFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);

    record Document(@JsonProperty("defaultPolicy") String defaultPolicy,
                    @JsonProperty("topics") Map<String, TopicRule> topics,
                    @JsonProperty("metrics") MetricsSettings metrics,
                    @JsonProperty("logging") LoggingSettings logging) {}

    record TopicRule(@JsonProperty("allowedSchemaIds") List<Integer> allowedSchemaIds) {}

    record MetricsSettings(@JsonProperty("includeSchemaIdLabel") Boolean includeSchemaIdLabel) {}

    record LoggingSettings(@JsonProperty("maxRejectLogsPerSecond") Integer maxRejectLogsPerSecond) {}

    private RulesLoader() {
    }

    public static GuardianRules load(Path file) {
        if (file == null) {
            throw new PluginConfigurationException("rulesFile must be configured");
        }
        try {
            long size = Files.size(file);
            if (size > MAX_FILE_BYTES) {
                throw new PluginConfigurationException("Rules file " + file + " is larger than " + MAX_FILE_BYTES + " bytes");
            }
            return parse(Files.readAllBytes(file), file.toString());
        }
        catch (NoSuchFileException e) {
            throw new PluginConfigurationException("Rules file " + file + " does not exist", e);
        }
        catch (IOException e) {
            throw new PluginConfigurationException("Rules file " + file + " cannot be read: " + e.getMessage(), e);
        }
    }

    public static GuardianRules parse(byte[] content, String sourceName) {
        Document document;
        try {
            document = MAPPER.readValue(content, Document.class);
        }
        catch (IOException e) {
            throw new PluginConfigurationException("Rules " + sourceName + " are not valid YAML/JSON or do not match the schema: " + e.getMessage(), e);
        }
        List<String> errors = new ArrayList<>();
        GuardianRules rules = validate(document, checksum(content), errors);
        if (!errors.isEmpty()) {
            throw new PluginConfigurationException("Invalid rules " + sourceName + ": " + String.join("; ", errors));
        }
        return rules;
    }

    private static GuardianRules validate(Document document, String checksum, List<String> errors) {
        if (document == null) {
            errors.add("document is empty");
            return null;
        }
        boolean policyValid = "DENY".equals(document.defaultPolicy()) || "ALLOW".equals(document.defaultPolicy());
        if (!policyValid) {
            errors.add("defaultPolicy is required and must be DENY or ALLOW but was " + document.defaultPolicy());
        }
        Map<String, SchemaIdSet> topics = new LinkedHashMap<>();
        if (document.topics() == null || document.topics().isEmpty()) {
            errors.add("at least one topic must be configured");
        }
        else {
            document.topics().forEach((name, rule) -> validateTopic(name, rule, topics, errors));
        }
        boolean includeSchemaIdLabel = document.metrics() != null && Boolean.TRUE.equals(document.metrics().includeSchemaIdLabel());
        int maxLogs = GuardianRules.DEFAULT_MAX_REJECT_LOGS_PER_SECOND;
        if (document.logging() != null && document.logging().maxRejectLogsPerSecond() != null) {
            maxLogs = document.logging().maxRejectLogsPerSecond();
            if (maxLogs < 0 || maxLogs > MAX_LOGS_PER_SECOND_LIMIT) {
                errors.add("logging.maxRejectLogsPerSecond must be between 0 and " + MAX_LOGS_PER_SECOND_LIMIT);
            }
        }
        return errors.isEmpty() ? new GuardianRules(Whitelist.of(topics, "ALLOW".equals(document.defaultPolicy())), includeSchemaIdLabel, maxLogs, checksum) : null;
    }

    private static void validateTopic(String name, TopicRule rule, Map<String, SchemaIdSet> topics, List<String> errors) {
        if (name == null || name.isBlank()) {
            errors.add("topic name must not be empty");
            return;
        }
        if (!TopicNames.isLegal(name)) {
            errors.add("topic name '" + name + "' is not a legal Kafka topic name");
            return;
        }
        if (rule == null || rule.allowedSchemaIds() == null || rule.allowedSchemaIds().isEmpty()) {
            errors.add("topic '" + name + "' must define at least one allowedSchemaIds entry");
            return;
        }
        int[] ids = new int[rule.allowedSchemaIds().size()];
        boolean valid = true;
        for (int i = 0; i < ids.length; i++) {
            Integer id = rule.allowedSchemaIds().get(i);
            if (id == null) {
                errors.add("topic '" + name + "' contains a null schema id");
                valid = false;
            }
            else if (id < 0) {
                errors.add("topic '" + name + "' contains a negative schema id: " + id);
                valid = false;
            }
            else {
                ids[i] = id;
            }
        }
        if (valid) {
            topics.put(name, SchemaIdSet.of(ids));
        }
    }

    private static String checksum(byte[] content) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(content);
            return HexFormat.of().formatHex(digest, 0, 6);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory on every Java platform", e);
        }
    }
}
