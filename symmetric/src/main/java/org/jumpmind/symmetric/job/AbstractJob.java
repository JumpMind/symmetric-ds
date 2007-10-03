package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;

abstract public class AbstractJob extends TimerTask {

    DataSource dataSource;
    
    @Override
    public void run() {
        try {
            //printDatabaseStats();
            doJob();
            //printDatabaseStats();
        } catch (Throwable ex) {
            getLogger().error(ex, ex);
        }
    }

    abstract void doJob() throws Exception;

    abstract Log getLogger();
    
    @SuppressWarnings("unused")
    private void printDatabaseStats() {
        if (getLogger().isDebugEnabled() && dataSource instanceof BasicDataSource) {
            BasicDataSource ds = (BasicDataSource)dataSource;
            getLogger().debug("There are currently " + ds.getNumActive() + " active database connections.");
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
