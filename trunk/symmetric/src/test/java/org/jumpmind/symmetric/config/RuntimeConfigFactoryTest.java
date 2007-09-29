package org.jumpmind.symmetric.config;

import org.testng.annotations.Test;

public class RuntimeConfigFactoryTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testRuntimeConfig() throws Exception {
        RuntimeConfigFactory r = new RuntimeConfigFactory();
        r.setDefaultInstance(new TestDefaultInstance());
        assert (r.isSingleton());
        assert (r.getObjectType().isAssignableFrom(IRuntimeConfig.class));
        assert (r.getObject() != null && r.getObject() instanceof IRuntimeConfig);
        r.setClassName("org.jumpmind.symmetric.PropertyRuntimeConfig");
        assert (r.getObject() != null && r.getObject() instanceof IRuntimeConfig);
    }

    class TestDefaultInstance implements IRuntimeConfig {

        public String getNodeGroupId() {
            return null;
        }

        public String getRegistrationUrl() {
            return null;
        }

        public String getExternalId() {
            return null;
        }

        public String getMyUrl() {
            return null;
        }

        public String getSchemaVersion() {
            return null;
        }

    }
}
