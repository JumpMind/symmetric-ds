package org.jumpmind.symmetric.service.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.log4j.Level;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractServiceTest {

    static protected ISymmetricEngine engine;

    protected final static Logger logger = LoggerFactory.getLogger(AbstractServiceTest.class);

    @BeforeClass
    public static void setup() {        
        if (engine == null) {
            SqlUtils.setCaptureOwner(true);
            try {
                Class<?> clazz = Class.forName("org.jumpmind.symmetric.test.TestSetupUtil");
                Method method = clazz.getMethod("prepareForServiceTests");
                engine = (ISymmetricEngine) method.invoke(clazz);
            } catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                logger.error(cause.getMessage(), cause);
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new IllegalStateException(cause);
                }
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                Assert.fail(ex.getMessage());
            }
        }
    }

    protected Level setLoggingLevelForTest(Level level) {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.jumpmind");
        Level old = logger.getLevel();
        logger.setLevel(level);
        return old;
    }

    protected void logTestRunning() {
        logger.info("Running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                + getSymmetricEngine().getSymmetricDialect().getPlatform().getName());
    }

    protected void logTestComplete() {
        logger.info("Completed running " + new Exception().getStackTrace()[1].getMethodName()
                + ". " + getSymmetricEngine().getSymmetricDialect().getPlatform().getName());
    }

    protected ISymmetricEngine getSymmetricEngine() {
        return engine;
    }

    protected IParameterService getParameterService() {
        return getSymmetricEngine().getParameterService();
    }

    protected ISymmetricDialect getDbDialect() {
        return getSymmetricEngine().getSymmetricDialect();
    }

    protected IConfigurationService getConfigurationService() {
        return getSymmetricEngine().getConfigurationService();
    }

    protected IRegistrationService getRegistrationService() {
        return getSymmetricEngine().getRegistrationService();
    }

    protected IDataExtractorService getDataExtractorService() {
        return getSymmetricEngine().getDataExtractorService();
    }

    protected IDataService getDataService() {
        return getSymmetricEngine().getDataService();
    }

    protected INodeService getNodeService() {
        return getSymmetricEngine().getNodeService();
    }

    protected IDatabasePlatform getPlatform() {
        return getSymmetricEngine().getSymmetricDialect().getPlatform();
    }

    protected IRouterService getRouterService() {
        return getSymmetricEngine().getRouterService();
    }

    protected ITriggerRouterService getTriggerRouterService() {
        return getSymmetricEngine().getTriggerRouterService();
    }

    protected IOutgoingBatchService getOutgoingBatchService() {
        return getSymmetricEngine().getOutgoingBatchService();
    }

    protected IIncomingBatchService getIncomingBatchService() {
        return getSymmetricEngine().getIncomingBatchService();
    }

    protected ISqlTemplate getSqlTemplate() {
        return getSymmetricEngine().getSymmetricDialect().getPlatform().getSqlTemplate();
    }

    protected IStagingManager getStagingManager() {
        return getSymmetricEngine().getStagingManager();
    }

    protected void assertTrue(boolean condition, String message) {
        Assert.assertTrue(message, condition);
    }

    protected void assertFalse(boolean condition, String message) {
        Assert.assertFalse(message, condition);
    }

    protected void assertNotNull(Object condition, String message) {
        Assert.assertNotNull(message, condition);
    }

    protected void assertNull(Object condition) {
        Assert.assertNull(condition);
    }

    protected void assertNotNull(Object condition) {
        Assert.assertNotNull(condition);
    }

    protected void assertNull(Object condition, String message) {
        Assert.assertNull(message, condition);
    }

    protected void assertFalse(boolean condition) {
        Assert.assertFalse(condition);
    }

    protected void assertTrue(boolean condition) {
        Assert.assertTrue(condition);
    }

    protected void assertEquals(Object actual, Object expected) {
        Assert.assertEquals(expected, actual);
    }

    protected void assertEquals(Object actual, Object expected, String message) {
        Assert.assertEquals(message, expected, actual);
    }

    protected void assertNotSame(Object actual, Object expected, String message) {
        Assert.assertNotSame(message, expected, actual);
    }

    protected void assertNumberOfRows(int rows, String tableName) {
        Assert.assertEquals(tableName + " had an unexpected number of rows", rows, getSqlTemplate()
                .queryForInt("select count(*) from " + tableName));
    }

    protected void forceRebuildOfTrigers() {
        getSqlTemplate().update("update sym_trigger set last_update_time=?", new Date());
        getTriggerRouterService().syncTriggers();
    }

    protected int countData() {
        return getDataService().countDataInRange(-1, Integer.MAX_VALUE);
    }

    protected String printDatabase() {
        return getSymmetricEngine().getSymmetricDialect().getPlatform().getName();
    }

    protected void assertNumberOfLinesThatStartWith(int expected, String startsWith, String text) {
        assertNumberOfLinesThatStartWith(expected, startsWith, text, false, false);
    }

    protected void assertNumberOfLinesThatStartWith(int expected, String startsWith, String text,
            boolean ignoreCase, boolean atLeast) {
        int actual = 0;
        String[] lines = text.split("\n");
        for (String line : lines) {
            if (line.startsWith(startsWith)) {
                actual++;
            } else if (ignoreCase && line.toLowerCase().startsWith(startsWith.toLowerCase())) {
                actual++;
            }
        }

        if (atLeast) {
            Assert.assertTrue(String.format(
                    "There was less than the expected (%d) number of occurrences of: %s", expected,
                    startsWith), actual >= expected);
        } else {
            Assert.assertEquals("There were not the expected number of occurrences of: "
                    + startsWith, expected, actual);
        }
    }

    protected void routeAndCreateGaps() {
        // one to route unrouted data
        getRouterService().routeData(true);
        // one to create gaps
        getRouterService().routeData(true);
    }

    protected void resetGaps() {
        getSqlTemplate().update("delete from sym_data_gap");
    }

    protected void resetBatches() {
        routeAndCreateGaps();
        getSqlTemplate().update("update sym_outgoing_batch set status='OK' where status != 'OK'");
        long startId = getSqlTemplate().queryForLong("select max(start_id) from sym_data_gap");
        getSqlTemplate()
                .update("update sym_data_gap set status='OK' where start_id != ?", startId);        
        checkForOpenResources();
    }
    
    protected void checkForOpenResources() {
        SqlUtils.logOpenResources();
        Assert.assertEquals("There should be no open cursors", 0, SqlUtils.getOpenSqlReadCursors().size());
        Assert.assertEquals("There should be no open transactions", 0, SqlUtils.getOpenTransactions().size());
    }

}
