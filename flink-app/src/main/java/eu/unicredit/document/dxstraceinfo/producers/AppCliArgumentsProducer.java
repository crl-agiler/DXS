package eu.unicredit.document.dxstraceinfo.producers;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.parser.ArgsParser;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.Objects;

@ApplicationScoped
public class AppCliArgumentsProducer {

    private static volatile String[] commandLineArgs;

    public static void initialize(String[] args) {
        Objects.requireNonNull(args, "args cannot be null");
        commandLineArgs = args.clone();
    }

    @Produces
    @ApplicationScoped
    public AppCliArguments cliArguments(
            ArgsParser argsParser) {

        String[] currentArgs = commandLineArgs;

        if (currentArgs == null) {
            throw new IllegalStateException(
                    "Command line arguments have not been initialized"
            );
        }
        return argsParser.parse(currentArgs.clone());
    }
}