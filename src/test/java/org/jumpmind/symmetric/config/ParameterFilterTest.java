package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Test;

public class ParameterFilterTest extends AbstractDatabaseTest {

    public ParameterFilterTest() throws Exception {
    }

    public ParameterFilterTest(String db) {
        super(db);
    }

    @Test
    public void testParameterFilter() {
        IParameterService service = find(Constants.PARAMETER_SERVICE);
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
