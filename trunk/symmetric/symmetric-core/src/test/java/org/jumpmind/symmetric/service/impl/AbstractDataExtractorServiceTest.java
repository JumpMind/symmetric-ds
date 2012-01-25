package org.jumpmind.symmetric.service.impl;

import java.io.StringWriter;

import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.junit.Test;

public abstract class AbstractDataExtractorServiceTest extends AbstractServiceTest {


    @Test
    public void testExtractConfigurationStandalone() throws Exception {
        IDataExtractorService service = getDataExtractorService();
        StringWriter writer = new StringWriter();
        service.extractConfigurationStandalone(TestConstants.TEST_CLIENT_NODE, writer);
        String content = writer.getBuffer().toString();
        assertNumberOfLinesThatStartWith(24, "table,", content);
        assertNumberOfLinesThatStartWith(14, "columns,", content);
        assertNumberOfLinesThatStartWith(14, "keys,", content);
        assertNumberOfLinesThatStartWith(14, "sql,", content);
        assertNumberOfLinesThatStartWith(0, "update,", content);
        assertNumberOfLinesThatStartWith(63, "insert,", content);
        assertNumberOfLinesThatStartWith(1, "commit,-9999", content);
        assertNumberOfLinesThatStartWith(1, "batch,-9999", content);
    }
    
}
