package eu.unicredit.document.dxstraceinfo.mapping;

import eu.unicredit.document.dxstraceinfo.avro.DocumentGroup;
import eu.unicredit.document.dxstraceinfo.avro.Signer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class SignerMapper
    implements MapFunction<Tuple2<DocumentGroup, Signer>, RowData> {

  @Override
  public RowData map(Tuple2<DocumentGroup, Signer> input) {

    DocumentGroup group = input.f0;
    Signer signer = input.f1;

    GenericRowData row = new GenericRowData(8);

    row.setField(0, signer.getId());

    row.setField(1, group.getId());

    row.setField(
        2,
        AvroRowDataConverters.string(
            signer.getNdg())
    );

    row.setField(
        3,
        AvroRowDataConverters.string(
            signer.getSignatureChannel())
    );

    row.setField(
        4,
        AvroRowDataConverters.string(
            signer.getMfaMethod())
    );

    row.setField(
        5,
        AvroRowDataConverters.string(
            signer.getAuthenticationId())
    );

    row.setField(
        6,
        AvroRowDataConverters.longValue(
            signer.getSignerReb())
    );

    row.setField(
        7,
        AvroRowDataConverters.timestamp(
            signer.getSignatureDate())
    );

    return row;
  }
}
