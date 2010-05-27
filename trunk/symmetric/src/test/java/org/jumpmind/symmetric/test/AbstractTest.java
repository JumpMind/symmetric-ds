package org.jumpmind.symmetric.test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.symmetric.load.csv.CsvLoader;
import org.jumpmind.symmetric.service.impl.DataLoaderService;
import org.jumpmind.symmetric.service.impl.RouterService;

abstract public class AbstractTest {

    protected Level setLoggingLevelForTest(Level level) {
        Level old = Logger.getLogger(getClass()).getLevel();
        Logger.getLogger(DataLoaderService.class).setLevel(level);
        Logger.getLogger(RouterService.class).setLevel(level);
        Logger.getLogger(CsvLoader.class).setLevel(level);
        return old;
    }
    
    protected void logTestRunning() {
        Logger.getLogger(getClass()).info("Running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                + printDatabases());
    }
    
    abstract protected String printDatabases();
    
}
