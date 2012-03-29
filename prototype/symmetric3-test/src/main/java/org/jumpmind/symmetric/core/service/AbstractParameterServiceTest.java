package org.jumpmind.symmetric.core.service;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.model.Parameters;
import org.junit.Test;

abstract public class AbstractParameterServiceTest extends AbstractServiceTest {

    @Test
    public void testSaveParameter() {
        client.getParameterService().saveParameter("test", "somevalue");
        Parameters parameters = client.getParameterService().getParameters();
        Assert.assertEquals("somevalue", parameters.get("test"));
    }
}
