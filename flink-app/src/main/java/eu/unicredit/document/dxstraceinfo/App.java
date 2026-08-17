package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.AdditionalContextProperty;
import eu.unicredit.document.dxstraceinfo.api.Pipeline;
import eu.unicredit.document.dxstraceinfo.api.SingleSourceLogicPipeline;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.config.DSXFlinkConfiguration;
import eu.unicredit.document.dxstraceinfo.config.GcpConfigAppRetriever;
import eu.unicredit.document.dxstraceinfo.context.CatalogLoaderProperty;
import eu.unicredit.document.dxstraceinfo.context.DiscardOutputTagProperty;
import eu.unicredit.document.dxstraceinfo.context.ErrorHandlerProperty;
import eu.unicredit.document.dxstraceinfo.context.SplitContextListProperty;
import eu.unicredit.document.dxstraceinfo.credentials.GcpCredentialsRetriever;
import eu.unicredit.document.dxstraceinfo.parser.JCommandParser;
import eu.unicredit.document.dxstraceinfo.pipeline.DSXKafkaSource;
import eu.unicredit.document.dxstraceinfo.pipeline.DSXProcessor;
import eu.unicredit.document.dxstraceinfo.pipeline.DXSSink;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class App {

    @VisibleForTesting
    public static List<AdditionalContextProperty> defaults() {
        return List.of(
                new DiscardOutputTagProperty(),
                new ErrorHandlerProperty(),
                new CatalogLoaderProperty(),
                new SplitContextListProperty()
        );
    }

    @VisibleForTesting
    public static Pipeline pipeline() {
        return SingleSourceLogicPipeline.<DossierTraceinfoEvent, RowData>builder()
                .source(new DSXKafkaSource())
                .processor(new DSXProcessor())
                .sink(new DXSSink())
                .jobName("dxs-traceinfo-flink-streaming-wl")
                .build();
    }

    public static void main(String[] args)
            throws Exception {
        DXSApplication dxsApplication = DXSApplication.builder()
                .argsParser(new JCommandParser())
                .configAppRetriever(new GcpConfigAppRetriever())
                .credentialsRetriever(new GcpCredentialsRetriever())
                .flinkConfiguration(new DSXFlinkConfiguration())
                .additionalContextProperties(defaults())
                .pipeline(pipeline())
                .bootstrap(args);
        dxsApplication.execute();
    }
}
