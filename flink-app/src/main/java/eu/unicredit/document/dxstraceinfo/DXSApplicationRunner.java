package eu.unicredit.document.dxstraceinfo;

import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import eu.unicredit.document.dxstraceinfo.config.ConfigApp;
import eu.unicredit.document.dxstraceinfo.config.GcpConfigAppRetriever;

public class DXSApplicationRunner {

    public static void run(String[] args) {
        AppCliArguments argv =
                AppCliArguments.parse(args);
        GcpConfigAppRetriever gcpConfigAppRetriever = new GcpConfigAppRetriever();
        ConfigApp appConfig = gcpConfigAppRetriever.getConfig(
                                argv.getBucketName(),
                                argv.getBaseConfigPath(),
                                argv.getEnvConfigPath());


    }
}
