package cz.company.guardian.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration of the {@code SchemaGuardian} filter inside the proxy configuration file.
 * The whitelist itself lives in a separate rules file so it can be shipped as a Kubernetes ConfigMap.
 *
 * @param rulesFile path to the rules YAML/JSON file, read once at start up
 */
public record GuardianConfig(@JsonProperty(value = "rulesFile", required = true) String rulesFile) {

    @JsonCreator
    public GuardianConfig {
    }
}
