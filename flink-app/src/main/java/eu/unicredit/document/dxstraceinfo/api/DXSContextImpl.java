package eu.unicredit.document.dxstraceinfo.api;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.pojo.ArtifactInformation;
import eu.unicredit.document.dxstraceinfo.config.pojo.ConfigApp;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DXSContextImpl implements DXSContext {

    private final StreamExecutionEnvironment env;
    private final ConfigApp config;
    private final AppCliArguments arguments;
    private final Credentials credentials;
    private final ArtifactInformation artifactInformation;
    private final Map<String, Object> context = new ConcurrentHashMap<>(4);

    public DXSContextImpl(StreamExecutionEnvironment env, ConfigApp config, AppCliArguments arguments, Credentials credentials, ArtifactInformation artifactInformation) {
        this.env = env;
        this.config = config;
        this.arguments = arguments;
        this.credentials = credentials;
        this.artifactInformation = artifactInformation;
    }

    @Override
    public StreamExecutionEnvironment streamingExecutionEnv() {
        return env;
    }

    @Override
    public ConfigApp config() {
        return config;
    }

    @Override
    public AppCliArguments args() {
        return arguments;
    }

    public Credentials credentials() {
        return credentials;
    }

    @Override
    public ArtifactInformation artifactInformation() {
        return this.artifactInformation;
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> type) {
        return Optional.ofNullable(this.context.get(name)).map(type::cast);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String name) {
        return Optional.ofNullable((T) this.context.get(name));
    }

    @Override
    public void add(String name, Object instance) {
        this.context.putIfAbsent(name, instance);
    }
}
