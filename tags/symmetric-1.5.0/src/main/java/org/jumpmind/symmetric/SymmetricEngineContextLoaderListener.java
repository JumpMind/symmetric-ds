/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * This is the standard way to bootstrap Symmetric in a web container. Symmetric
 * uses Spring's WebApplicationContext for access to symmetric from its
 * Servlets. This servlet context listener forces the contextConfigLocation for
 * Spring to be load symmetric.xml. <p/> Developers have the option to subclass
 * off of this listener and override the createConfigureAndStartEngine() method.
 */
public class SymmetricEngineContextLoaderListener extends ContextLoaderListener {

    static final String SYMMETRIC_SPRING_LOCATION = "classpath:/symmetric.xml";

    static final Log logger = LogFactory.getLog(SymmetricEngineContextLoaderListener.class);

    SymmetricEngine engine = null;

    public SymmetricEngineContextLoaderListener() {
    }

    public SymmetricEngineContextLoaderListener(SymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    final public void contextInitialized(ServletContextEvent event) {
        try {
            super.contextInitialized(event);
            ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(event.getServletContext());
            createConfigureAndStartEngine(ctx);
        } catch (Exception ex) {
            logger.error("Failed to initialize the web server context.", ex);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        if (engine != null) {
            engine.stop();
            engine = null;
        }
    }

    protected void createConfigureAndStartEngine(ApplicationContext ctx) {
        if (this.engine == null) {
            this.engine = new SymmetricEngine(ctx);
        }
        engine.start();
    }

    @Override
    protected ContextLoader createContextLoader() {
        return new ContextLoader() {
            @SuppressWarnings("unchecked")
            protected WebApplicationContext createWebApplicationContext(ServletContext servletContext,
                    ApplicationContext parent) throws BeansException {

                Class contextClass = determineContextClass(servletContext);
                if (!ConfigurableWebApplicationContext.class.isAssignableFrom(contextClass)) {
                    throw new ApplicationContextException("Custom context class [" + contextClass.getName()
                            + "] is not of type ConfigurableWebApplicationContext");
                }

                ConfigurableWebApplicationContext wac = (ConfigurableWebApplicationContext) BeanUtils
                        .instantiateClass(contextClass);
                wac.setParent(parent);
                wac.setServletContext(servletContext);
                String configLocation = servletContext.getInitParameter(CONFIG_LOCATION_PARAM);
                if (configLocation == null) {
                    configLocation = SYMMETRIC_SPRING_LOCATION;
                } else if (!configLocation.contains(SYMMETRIC_SPRING_LOCATION)) {
                    configLocation = SYMMETRIC_SPRING_LOCATION + "," + configLocation;
                }
                wac.setConfigLocations(StringUtils.tokenizeToStringArray(configLocation,
                        ConfigurableWebApplicationContext.CONFIG_LOCATION_DELIMITERS));

                wac.refresh();
                return wac;
            }
        };
    }

    public SymmetricEngine getEngine() {
        return engine;
    }

}
