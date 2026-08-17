package eu.unicredit.document.dxstraceinfo.api;

import org.apache.flink.util.OutputTag;

import java.io.Serializable;

public interface ErrorHandler<T> extends Serializable {

    void handle(T record, String error, OutputEmitter emitter);

    static <T> ErrorHandler<T> noop() {
        return (r, e, emitter) -> {};
    }

    @FunctionalInterface
    interface OutputEmitter extends Serializable {
         <X> void emit(OutputTag<X> outputTag, X value);
    }
}
