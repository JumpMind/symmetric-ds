package org.jumpmind.symmetric.config;

import java.net.URL;

import org.jumpmind.symmetric.config.PropertyRuntimeConfig;
import org.testng.annotations.Test;

public class PropertyRuntimeConfigurationTest {

    @Test
    public void testGetHostUrl() throws Exception {
        PropertyRuntimeConfig p = new PropertyRuntimeConfig();
        p.setRegistrationUrlString("http://localhost:8080");
        String url = p.getRegistrationUrl();
        assert (url != null);
        assert (new URL(url).getHost().equals("localhost"));
    }

}
