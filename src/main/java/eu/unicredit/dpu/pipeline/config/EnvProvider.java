// src/main/java/eu/unicredit/dpu/pipeline/config/EnvProvider.java
package eu.unicredit.dpu.pipeline.config;

@FunctionalInterface
public interface EnvProvider {
    String getenv(String name);

    static EnvProvider system() {
        return System::getenv;
    }
}