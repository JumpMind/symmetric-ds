package org.jumpmind.db;

import org.jumpmind.util.Log4jLog;
import org.jumpmind.util.LogFactory;
import org.junit.BeforeClass;



abstract public class AbstractDbTest {
    
    @BeforeClass
    public static void setupLogger() {
        LogFactory.setLogClass(Log4jLog.class);
    }


}
