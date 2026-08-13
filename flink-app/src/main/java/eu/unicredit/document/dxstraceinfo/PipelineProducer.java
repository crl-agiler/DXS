package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.Pipeline;
import eu.unicredit.document.dxstraceinfo.api.SingleSourceLogicPipeline;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import org.apache.flink.table.data.RowData;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

@ApplicationScoped
public class PipelineProducer {

    @Singleton
    @Produces
    public Pipeline pipeline(DSXFlinkConfiguration flinkConfiguration,
                             DSXKafkaSource kafkaSource,
                             DSXProcessor processor,
                             DXSSink sink,
                             ConfigApp configApp) {
       return SingleSourceLogicPipeline.<DossierTraceinfoEvent, RowData>builder()
                        .flinkConfiguration(flinkConfiguration)
                        .source(kafkaSource)
                        .processor(processor)
                        .sink(sink)
                .jobName(configApp.getFlinkConfig().getJobName())
                .build();
    }

}
