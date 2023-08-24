package org.jumpmind.symmetric;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.web.SymmetricContextListener;
import org.jumpmind.symmetric.web.SymmetricServlet;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = { "org.jumpmind.symmetric", "com.jumpmind.symmetric" })
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
        ServletRegistrationBean<SymmetricServlet> bean = new ServletRegistrationBean<>(new SymmetricServlet(),
                "/sync/*");
        bean.setLoadOnStartup(1);
        return bean;
    }

    public static void main(String[] args) {
        System.setProperty(SystemConstants.SYSPROP_STANDALONE_WEB, "true");
        new SpringApplicationBuilder().registerShutdownHook(false).listeners(new SymmetricBootPropertySetupListener())
                .bannerMode(Banner.Mode.OFF).sources(SymmetricBoot.class).run(args);
    }
}
