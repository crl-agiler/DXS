package eu.unicredit.document.dxstraceinfo.parser;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;

public interface ArgsParser {
    AppCliArguments parse(String[] args);

}
