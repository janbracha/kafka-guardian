package cz.company.guardian;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.micrometer.core.instrument.Metrics;

import cz.company.guardian.config.GuardianConfig;
import cz.company.guardian.config.GuardianRules;
import cz.company.guardian.config.RulesLoader;
import cz.company.guardian.logging.LogRateLimiter;
import cz.company.guardian.logging.RejectLogger;
import cz.company.guardian.metrics.GuardianMetrics;
import cz.company.guardian.validation.RequestValidator;

/**
 * Entry point discovered by Kroxylicious through {@link java.util.ServiceLoader}; referenced as
 * {@code type: SchemaGuardian} in the proxy configuration.
 * <p>
 * The rules are loaded and validated exactly once in {@link #initialize}. An invalid or missing rules file makes the
 * proxy fail to start. There is no hot reload: changing the rules means rolling the pods.
 */
@Plugin(configType = GuardianConfig.class)
public class SchemaGuardian implements FilterFactory<GuardianConfig, GuardianRuntime> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGuardian.class);

    @Override
    public GuardianRuntime initialize(FilterFactoryContext context, GuardianConfig config) throws PluginConfigurationException {
        GuardianConfig validConfig = Plugins.requireConfig(this, config);
        if (validConfig.rulesFile() == null || validConfig.rulesFile().isBlank()) {
            throw new PluginConfigurationException("SchemaGuardian requires 'rulesFile'");
        }
        Path file;
        try {
            file = Path.of(validConfig.rulesFile());
        }
        catch (InvalidPathException e) {
            throw new PluginConfigurationException("SchemaGuardian 'rulesFile' is not a valid path", e);
        }
        GuardianRules rules = RulesLoader.load(file);
        GuardianMetrics metrics = new GuardianMetrics(Metrics.globalRegistry, rules.whitelist(), rules.includeSchemaIdLabel(), rules.checksum());
        LOGGER.atInfo()
                .addKeyValue("event", "rules_loaded")
                .addKeyValue("topics", rules.whitelist().topicCount())
                .addKeyValue("checksum", rules.checksum())
                .log("rules_loaded topics={} checksum={}", rules.whitelist().topicCount(), rules.checksum());
        return new GuardianRuntime(
                new RequestValidator(rules.whitelist()),
                metrics,
                new RejectLogger(new LogRateLimiter(rules.maxRejectLogsPerSecond())));
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, GuardianRuntime runtime) {
        return new SchemaGuardianFilter(runtime);
    }
}
