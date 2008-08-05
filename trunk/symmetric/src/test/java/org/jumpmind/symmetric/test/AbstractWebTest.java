package org.jumpmind.symmetric.test;

import javax.servlet.ServletContext;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;

public class AbstractWebTest {

    protected static ConfigurableWebApplicationContext applicationContext;
    protected static ServletContext servletContext;
    
    @BeforeClass
    public static void springTestContextBeforeTestMethod() throws Exception {
        servletContext = new MockServletContext();
        applicationContext = new XmlWebApplicationContext();
        TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT,
                null, "hsqldb");
        IBootstrapService bootstrapService = AppUtils
                .find(Constants.BOOTSTRAP_SERVICE, TestSetupUtil.getRootEngine());
        bootstrapService.setupDatabase();
        applicationContext.setParent(TestSetupUtil.getRootEngine().getApplicationContext());
        applicationContext.setServletContext(servletContext);
        applicationContext.setConfigLocations(new String[0]);
        applicationContext.refresh();
        servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, applicationContext);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        TestSetupUtil.cleanup();
    }
}
