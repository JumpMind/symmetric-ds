package org.jumpmind.symmetric;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ServerConstants;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class SymmetricBootPropertySetupListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        TypedProperties serverProperties = new TypedProperties(System.getProperties());
        ConfigurableEnvironment environment = event.getEnvironment();
        Properties props = new Properties();
        String httpPort = serverProperties.getProperty(ServerConstants.HTTP_PORT, "31415");
        String httpsPort = serverProperties.getProperty(ServerConstants.HTTPS_PORT, null);
        if (isNotBlank(httpPort)) {
            props.put("server.port", httpPort);
        }
        environment.getPropertySources().addFirst(new PropertiesPropertySource("myProps", props));
    }
}
