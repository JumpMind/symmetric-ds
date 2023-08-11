package org.jumpmind.symmetric;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ServerConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
import org.jumpmind.symmetric.web.*;
import org.jumpmind.util.AppUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.util.Set;

@SpringBootApplication(scanBasePackages = {"org.jumpmind.symmetric", "com.jumpmind.symmetric"})
public class SymmetricBoot {

    @Bean
    ServletContextInitializer servletContextInitializer() {
        return new ServletContextInitializer() {
            @Override
            public void onStartup(ServletContext servletContext) throws ServletException {
                servletContext.setInitParameter(WebConstants.INIT_PARAM_AUTO_START, "true");
                servletContext.setInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE, "true");
            }
        };
    }

    @Bean
    SymmetricContextListener symmetricContextListener() {
        return new SymmetricContextListener();
    }

    @Bean
    ServletRegistrationBean<SymmetricServlet> symmetricServlet() {
        ServletRegistrationBean<SymmetricServlet> bean = new ServletRegistrationBean<>(new SymmetricServlet(), "/sync/*");
        bean.setLoadOnStartup(1);
        return bean;
    }

    public static void main(String[] args) {
        System.setProperty(SystemConstants.SYSPROP_STANDALONE_WEB, "true");
        new SpringApplicationBuilder()
                .registerShutdownHook(false)
                .listeners(new SymmetricBootPropertySetupListener())
                .bannerMode(Banner.Mode.OFF)
                .sources(SymmetricBoot.class).run(args);
    }
}
