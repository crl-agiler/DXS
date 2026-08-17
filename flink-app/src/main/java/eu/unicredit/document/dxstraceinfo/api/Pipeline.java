package eu.unicredit.document.dxstraceinfo.api;

public interface Pipeline {

    String jobName();

    void run(DXSContext context) throws Exception;

}
