package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.Pipeline;
import eu.unicredit.document.dxstraceinfo.producers.AppCliArgumentsProducer;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class DXSApplication {

    private final Pipeline pipeline;

    public static void run(String[] args) throws Exception {
            AppCliArgumentsProducer.initialize(args);
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment
                            .getExecutionEnvironment();
            try (WeldContainer container =
                         new Weld().initialize()) {
                DXSApplication dxsApplication = container.select(DXSApplication.class)
                        .get();
                dxsApplication.execute(env);
            }
    }

    @VisibleForTesting
    public void execute(StreamExecutionEnvironment env) throws Exception {
        pipeline.run(env);
    }

}
