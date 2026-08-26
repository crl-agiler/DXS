package eu.unicredit.document.dxstraceinfo.pipeline;

import eu.unicredit.document.dxstraceinfo.api.DXSContext;
import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.api.Processor;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import eu.unicredit.document.dxstraceinfo.transform.SplitTransformLogic;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.util.List;

@Slf4j
@SuppressWarnings("unchecked")
public class DSXProcessor implements Processor<DossierTraceinfoEvent, RowData> {

    @Override
    public DataStream<RowData> process(DataStream<DossierTraceinfoEvent> input, DXSContext context) {
        ErrorHandler<DossierTraceinfoEvent> errorHandler = (ErrorHandler<DossierTraceinfoEvent>) context.get("ErrorHandler").orElseThrow();
        List<SplitContext<?>> splitContexts = (List<SplitContext<?>>) context.get("SplitContextList").orElseThrow();
        SplitTransformLogic splitTransformLogic = new SplitTransformLogic(splitContexts, errorHandler);
        KeySelector<DossierTraceinfoEvent, Long> getDossierId = DossierTraceinfoEvent::getDossierId;
        KeyedStream<DossierTraceinfoEvent, Long> dossierTraceinfoEventStringKeyedStream = input.keyBy(getDossierId);
        SingleOutputStreamOperator<RowData> outputStreamOperator = dossierTraceinfoEventStringKeyedStream.process(splitTransformLogic);
        return outputStreamOperator.name("SplitTransformLogic").uid("transform-transform-logic");
    }

    @Override
    public void onInit() {
        log.info("Initializing DSX Processor");
    }
}
