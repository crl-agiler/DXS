package eu.unicredit.document.dxstraceinfo.parser;

import com.beust.jcommander.JCommander;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;

@Slf4j
@ApplicationScoped
public class JCommandParser implements ArgsParser {

    @Override
    public AppCliArguments parse(String[] args) {
        AppCliArguments obj = new AppCliArguments();
        JCommander.newBuilder().addObject(obj).build().parse(args);
        log.info("Arguments read: [{}]", obj);
        return obj;
    }

}
