package eu.unicredit.document.dxstraceinfo.api;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ArtifactInformation;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.credentials.Credentials;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Optional;

public interface DXSContext extends Serializable {

    StreamExecutionEnvironment streamingExecutionEnv();
    ConfigApp config();
    AppCliArguments args();
    Credentials credentials();
    ArtifactInformation artifactInformation();
    <T> Optional<T> get(String name, Class<T> type);
    <T> Optional<T> get(String name);
    void add(String name, Object instance);

}
