package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.Bootstrap;
import eu.unicredit.document.dxstraceinfo.api.ConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.FlinkConfiguration;
import eu.unicredit.document.dxstraceinfo.api.Pipeline;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.context.DXSContextImpl;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import eu.unicredit.document.dxstraceinfo.credentials.CredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.parser.ArgsParser;
import lombok.ToString;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class DXSApplication {

    private final Pipeline pipeline;
    private final DXSContext context;

    DXSApplication(Pipeline pipeline, DXSContext context) {
        this.pipeline = pipeline;
        this.context = context;
    }

    public static DXSApplicationBuilder builder() {
        return new DXSApplicationBuilder();
    }

    public void execute() throws Exception {
        pipeline.run(context);
    }

    @ToString
    public static class DXSApplicationBuilder implements Bootstrap<DXSApplication> {
        private Pipeline pipeline;
        private ArgsParser argsParser;
        private CredentialsRetriever credentialsRetriever;
        private ConfigAppRetriever configAppRetriever;
        private List<AdditionalContextProperty> additionalContextProperties = Collections.emptyList();
        private FlinkConfiguration flinkConfiguration = FlinkConfiguration.NONE;
        private DXSContext context;

        DXSApplicationBuilder() {
        }

        public DXSApplicationBuilder pipeline(Pipeline pipeline) {
            this.pipeline = pipeline;
            return this;
        }

        public DXSApplicationBuilder argsParser(ArgsParser argsParser) {
            this.argsParser = argsParser;
            return this;
        }

        public DXSApplicationBuilder credentialsRetriever(CredentialsRetriever credentialsRetriever) {
            this.credentialsRetriever = credentialsRetriever;
            return this;
        }

        public DXSApplicationBuilder configAppRetriever(ConfigAppRetriever configAppRetriever) {
            this.configAppRetriever = configAppRetriever;
            return this;
        }

        public DXSApplicationBuilder additionalContextProperties(List<AdditionalContextProperty> additionalContextProperties) {
            this.additionalContextProperties = additionalContextProperties;
            return this;
        }

        public DXSApplicationBuilder flinkConfiguration(FlinkConfiguration flinkConfiguration) {
            this.flinkConfiguration = flinkConfiguration;
            return this;
        }

        public DXSApplicationBuilder context(DXSContext context) {
            this.context = context;
            return this;
        }

        @Override
        public DXSApplication bootstrap(String[] args) throws Exception {
                AppCliArguments arguments = argsParser.parse(args);
                ConfigApp config = configAppRetriever.getConfig();
                StreamExecutionEnvironment env =
                        StreamExecutionEnvironment
                                .getExecutionEnvironment();
                String projectId = config.getProjectId();
                String secretId = config
                        .getSchemaRegistryConfig()
                        .getSecretId();
                Credentials credentials =
                        credentialsRetriever.getCredentials(
                                projectId,
                                secretId);
                this.context = new DXSContextImpl(env, config, arguments, credentials);
                flinkConfiguration.configure(context);
                this.additionalContextProperties.forEach(e -> context.add(e.name(), e.instance(context)));
            return new DXSApplication(this.pipeline, this.context);
        }
    }
}
