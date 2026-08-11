package eu.unicredit.document.dxstraceinfo.api;

public interface Module {

    String name();

    void configure(ModuleContext context);

}
