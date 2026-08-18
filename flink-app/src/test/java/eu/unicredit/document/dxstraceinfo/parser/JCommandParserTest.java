package eu.unicredit.document.dxstraceinfo.parser;

import com.beust.jcommander.ParameterException;
import eu.unicredit.document.dxstraceinfo.config.AppCliArguments;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JCommandParserTest {

    @Test
    void shouldParseArguments() {

        JCommandParser parser =
                new JCommandParser();

        AppCliArguments result =
                parser.parse(new String[]{
                        "--bucketName", "test-bucket",
                        "--baseConfigPath", "/0/dataproduct/",
                        "--envConfigPath", "config-test.yaml"
                });

        assertNotNull(result);
        assertEquals(
                "test-bucket",
                result.getBucketName()
        );
        assertEquals("/0/dataproduct/", result.getBaseConfigPath());
        assertEquals("config-test.yaml", result.getEnvConfigPath());
    }

    @Test
    void shouldParseEmptyArguments() {

        JCommandParser parser =
                new JCommandParser();

        Assertions.assertThrows(ParameterException.class,
                () -> parser.parse(new String[0]));

    }
}