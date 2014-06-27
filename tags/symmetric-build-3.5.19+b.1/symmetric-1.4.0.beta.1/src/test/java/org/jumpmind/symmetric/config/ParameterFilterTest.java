package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.service.IParameterService;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParameterFilterTest extends AbstractDatabaseTest {

    @Test(groups = "continuous")
    public void testParameterFilter() {
        IParameterService service = getParameterService();
        service.setParameterFilter(new IParameterFilter() {
            public String filterParameter(String key, String value) {
                if (key.equals("param.filter.test")) {
                    return "gotcha";
                } else {
                    return value;
                }
            }

            public boolean isAutoRegister() {
                return false;
            }
        });

        Assert.assertEquals(service.getString("param.filter.test"), "gotcha");
        Assert.assertEquals(service.getExternalId(), "00000");
    }
}
