package org.jumpmind.symmetric;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextException;
import org.springframework.util.StringUtils;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * This is the standard way to bootstrap Symmetric in a web container.  Symmetric uses
 * Spring's WebApplicationContext for access to symmetric from its Servlets.  
 * This servlet context listener forces the contextConfigLocation for Spring to be load 
 * symmetric.xml.
 * <p/>
 * Developers have the option to subclass off of this listener and override the createConfigureAndStartEngine()
 * method.
 */
public class SymmetricEngineContextLoaderListener extends ContextLoaderListener {

    static final String SYMMETRIC_SPRING_LOCATION = "classpath:/symmetric.xml";

    @Override
    final public void contextInitialized(ServletContextEvent event) {
        super.contextInitialized(event);
        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(event.getServletContext());
        createConfigureAndStartEngine(ctx);
    }

    protected void createConfigureAndStartEngine(ApplicationContext ctx) {
        SymmetricEngine engine = new SymmetricEngine(ctx);
        engine.start();
    }

    @Override
    protected ContextLoader createContextLoader() {
        return new ContextLoader() {
            @SuppressWarnings("unchecked")
            protected WebApplicationContext createWebApplicationContext(
                    ServletContext servletContext, ApplicationContext parent)
                    throws BeansException {

                Class contextClass = determineContextClass(servletContext);
                if (!ConfigurableWebApplicationContext.class
                        .isAssignableFrom(contextClass)) {
                    throw new ApplicationContextException(
                            "Custom context class ["
                                    + contextClass.getName()
                                    + "] is not of type ConfigurableWebApplicationContext");
                }

                ConfigurableWebApplicationContext wac = (ConfigurableWebApplicationContext) BeanUtils
                        .instantiateClass(contextClass);
                wac.setParent(parent);
                wac.setServletContext(servletContext);
                String configLocation = servletContext
                        .getInitParameter(CONFIG_LOCATION_PARAM);
                if (configLocation == null) {
                    configLocation = SYMMETRIC_SPRING_LOCATION;
                } else if (!configLocation.contains(SYMMETRIC_SPRING_LOCATION)) {
                    configLocation = SYMMETRIC_SPRING_LOCATION + ","
                            + configLocation;
                }
                wac
                        .setConfigLocations(StringUtils
                                .tokenizeToStringArray(
                                        configLocation,
                                        ConfigurableWebApplicationContext.CONFIG_LOCATION_DELIMITERS));

                wac.refresh();
                return wac;
            }
        };
    }

}
