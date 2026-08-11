package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.api.ErrorHandler;
import eu.unicredit.document.dxstraceinfo.api.Processor;
import eu.unicredit.document.dxstraceinfo.avro.DossierTraceinfoEvent;
import eu.unicredit.document.dxstraceinfo.handler.FlinkSimpleErrorHandler;
import eu.unicredit.document.dxstraceinfo.transform.PreKeyFilterProcess;
import eu.unicredit.document.dxstraceinfo.transform.SplitContext;
import eu.unicredit.document.dxstraceinfo.transform.SplitTransformLogic;
import lombok.AllArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import java.util.List;

@AllArgsConstructor
public class DSXProcessor implements Processor<DossierTraceinfoEvent, RowData> {

    private final OutputTag<RowData> discardTag;
    private final List<SplitContext<?>> splitContexts;

    @Override
    public DataStream<RowData> process(DataStream<DossierTraceinfoEvent> input) {
        ErrorHandler<DossierTraceinfoEvent> errorHandler = new FlinkSimpleErrorHandler(discardTag);
        PreKeyFilterProcess dossierIdNotNullProcess = new PreKeyFilterProcess(errorHandler);
        SplitTransformLogic splitTransformLogic = new SplitTransformLogic(splitContexts, errorHandler);
        SingleOutputStreamOperator<DossierTraceinfoEvent> process = input.process(dossierIdNotNullProcess);
        KeyedStream<DossierTraceinfoEvent, String> dossierTraceinfoEventStringKeyedStream = process.keyBy(DossierTraceinfoEvent::getMasterDossierId);
        SingleOutputStreamOperator<RowData> outputStreamOperator = dossierTraceinfoEventStringKeyedStream.process(splitTransformLogic);
        return outputStreamOperator.name("SplitTransformLogic").uid("transform-transform-logic");
    }

    @Override
    public void onInit() {

    }
}
