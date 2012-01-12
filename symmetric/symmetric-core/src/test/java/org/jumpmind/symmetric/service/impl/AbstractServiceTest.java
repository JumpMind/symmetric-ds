package org.jumpmind.symmetric.service.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.log.Log4jLog;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.TestUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IConfigurationService;
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

public abstract class AbstractServiceTest {

    static private ISymmetricEngine engine;

    static {
        org.jumpmind.log.LogFactory.setLogClass(Log4jLog.class);
    }

    @BeforeClass
    public static void setup() {
        if (engine == null) {
            try {
                Class<?> clazz = Class.forName("org.jumpmind.symmetric.test.TestSetupUtil");
                Method method = clazz.getMethod("prepareForServiceTests");
                engine = (ISymmetricEngine) method.invoke(clazz);
            } catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                TestUtils.getLog().error(cause);
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new IllegalStateException(cause);
                }
            } catch (Exception ex) {
                TestUtils.getLog().error(ex);
                Assert.fail(ex.getMessage());
            }
        }
    }

    protected Level setLoggingLevelForTest(Level level) {        
        Logger logger = Logger.getLogger(getSymmetricEngine().getLog().getCategory());
        Level old = logger.getLevel();
        logger.setLevel(level);
        return old;
    }

    protected void logTestRunning() {
        TestUtils.getLog().info(
                "Running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                        + getSymmetricEngine().getSymmetricDialect().getPlatform().getName());
    }

    protected void logTestComplete() {
        TestUtils.getLog().info(
                "Completed running " + new Exception().getStackTrace()[1].getMethodName() + ". "
                        + getSymmetricEngine().getSymmetricDialect().getPlatform().getName());
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

    protected IDataService getDataService() {
        return getSymmetricEngine().getDataService();
    }

    protected INodeService getNodeService() {
        return getSymmetricEngine().getNodeService();
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
}
