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

import java.util.ArrayList;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import mx4j.tools.adaptor.http.HttpAdaptor;
import mx4j.tools.adaptor.http.XSLTProcessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.web.SymmetricFilter;
import org.jumpmind.symmetric.web.SymmetricServlet;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Start up SymmetricDS through an embedded Jetty instance.
 * 
 * @see SymmetricLauncher#main(String[])
 */
public class SymmetricWebServer implements ApplicationContextAware {

    protected static final Log logger = LogFactory.getLog(SymmetricWebServer.class);

    public enum Mode {
        HTTP, HTTPS, MIXED;
    }

    protected SymmetricEngineContextLoaderListener contextListener;

    protected Server server;

    protected boolean join = true;

    protected boolean createJmxServer = true;

    protected String webHome = "/sync";

    protected int maxIdleTime = 900000;

    protected int httpPort = -1;

    protected int httpsPort = -1;

    protected String propertiesFile;
    
    protected String host;

    /**
     * This will only be set if the SymmetricWebServer itself is created from a
     * Spring context.
     */
    protected ApplicationContext parentContext;

    public SymmetricWebServer() {
    }

    public SymmetricWebServer(SymmetricEngine engine) {
        this.contextListener = new SymmetricEngineContextLoaderListener(engine);
    }

    public SymmetricWebServer(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }
    
    public SymmetricWebServer(int maxIdleTime, String propertiesUrl) {
        this.propertiesFile = propertiesUrl;
        this.maxIdleTime = maxIdleTime;
    }    

    public void start(int port, String propertiesUrl) throws Exception {
        this.propertiesFile = propertiesUrl;
        start(port);
    }

    public SymmetricEngine getEngine() {
        if (contextListener == null) {
            contextListener = new SymmetricEngineContextLoaderListener(new SymmetricEngine(parentContext,
                    propertiesFile));
        }
        return contextListener.getEngine();
    }

    public void start() throws Exception {
        if (httpPort > 0 && httpsPort > 0) {
            startMixed(httpPort, httpsPort);
        } else if (httpPort > 0) {
            start(httpPort);
        } else if (httpsPort > 0) {
            startSecure(httpsPort);
        } else {
            throw new IllegalStateException("Either an http or https port needs to be set before starting the server.");
        }
    }

    public void start(int port) throws Exception {
        start(port, 0, Mode.HTTP);
    }

    public void startSecure(int port) throws Exception {
        start(0, port, Mode.HTTPS);
    }

    public void startMixed(int port, int securePort) throws Exception {
        start(port, securePort, Mode.MIXED);
    }

    public void start(int port, int securePort, Mode mode) throws Exception {
        getEngine();
        server = new Server();
        server.setConnectors(getConnectors(port, securePort, mode));

        Context webContext = new Context(server, webHome, Context.NO_SESSIONS);

        if (this.contextListener == null) {
            this.contextListener = new SymmetricEngineContextLoaderListener();
        }

        webContext.addEventListener(this.contextListener);

        webContext.addFilter(SymmetricFilter.class, "/*", 0);

        ServletHolder servletHolder = new ServletHolder(SymmetricServlet.class);
        servletHolder.setInitOrder(0);
        webContext.addServlet(servletHolder, "/*");

        server.addHandler(webContext);
        server.start();

        if (createJmxServer) {
            int httpJmxPort = port != 0 ? port + 1 : securePort + 1;
            registerHttpJmxAdaptor(httpJmxPort);
        }

        if (join) {
            server.join();
        }
    }

    protected Connector[] getConnectors(int port, int securePort, Mode mode) {
        ArrayList<Connector> connectors = new ArrayList<Connector>();
        String keyStoreFile = System.getProperty("sym.keystore.file");

        if (mode.equals(Mode.HTTP) || mode.equals(Mode.MIXED)) {
            Connector connector = new SelectChannelConnector();
            connector.setPort(port);
            connector.setHost(host);
            ((SelectChannelConnector) connector).setMaxIdleTime(maxIdleTime);
            connectors.add(connector);
            logger.info("About to start SymmetricDS web server on port " + port);
        }
        if (mode.equals(Mode.HTTPS) || mode.equals(Mode.MIXED)) {
            Connector connector = new SslSocketConnector();
            connector.setHost(host);
            ((SslSocketConnector) connector).setKeystore(keyStoreFile);
            ((SslSocketConnector) connector).setPassword("changeit");
            ((SslSocketConnector) connector).setMaxIdleTime(maxIdleTime);
            connector.setPort(securePort);
            connectors.add(connector);
            logger.info("About to start SymmetricDS web server on secure port " + securePort);
        }
        return connectors.toArray(new Connector[connectors.size()]);
    }

    protected void registerHttpJmxAdaptor(int jmxPort) throws Exception {
        IParameterService parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, getEngine());
        if (parameterService.is(ParameterConstants.JMX_HTTP_CONSOLE_ENABLED)) {
            logger.info("Starting JMX HTTP console on port " + jmxPort);
            MBeanServer mbeanServer = AppUtils.find(Constants.MBEAN_SERVER, getEngine());
            ObjectName name = getHttpJmxAdaptorName();
            mbeanServer.createMBean(HttpAdaptor.class.getName(), name);
            mbeanServer.setAttribute(name, new Attribute("Port", new Integer(jmxPort)));
            ObjectName processorName = getXslJmxAdaptorName();
            mbeanServer.createMBean(XSLTProcessor.class.getName(), processorName);
            mbeanServer.setAttribute(name, new Attribute("ProcessorName", processorName));
            mbeanServer.invoke(name, "start", null, null);
        }
    }

    protected ObjectName getHttpJmxAdaptorName() throws MalformedObjectNameException {
        return new ObjectName("Server:name=HttpAdaptor");
    }

    protected ObjectName getXslJmxAdaptorName() throws MalformedObjectNameException {
        return new ObjectName("Server:name=XSLTProcessor");
    }

    protected void removeHttpJmxAdaptor() {
        IParameterService parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, getEngine());
        if (parameterService.is(ParameterConstants.JMX_HTTP_CONSOLE_ENABLED)) {
            try {
                MBeanServer mbeanServer = AppUtils.find(Constants.MBEAN_SERVER, getEngine());
                mbeanServer.unregisterMBean(getHttpJmxAdaptorName());
                mbeanServer.unregisterMBean(getXslJmxAdaptorName());
            } catch (Exception e) {
                logger.warn("Could not unregister the JMX HTTP Adaptor");
            }
        }
    }

    public void stop() throws Exception {
        if (server != null) {
            if (createJmxServer) {
                removeHttpJmxAdaptor();
            }
            server.stop();
        }
    }

    public SymmetricEngineContextLoaderListener getContextListener() {
        return contextListener;
    }

    /**
     * Before starting the web server, you have the option of overriding the
     * default context listener.
     * 
     * @param contextListener
     *            Usually an overridden instance
     */
    public void setContextListener(SymmetricEngineContextLoaderListener contextListener) {
        this.contextListener = contextListener;
    }

    public static void main(String[] args) throws Exception {
        new SymmetricWebServer().start(8080);
    }

    public boolean isJoin() {
        return join;
    }

    public void setJoin(boolean join) {
        this.join = join;
    }

    public void setWebHome(String webHome) {
        this.webHome = webHome;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public void setHttpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
    }

    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    /**
     * @see ApplicationContextAware
     */
    public void setApplicationContext(ApplicationContext context) {
        this.parentContext = context;
    }

    public void setCreateJmxServer(boolean createJmxServer) {
        this.createJmxServer = createJmxServer;
    }
    
    public void setHost(String host) {
        this.host = host;
    }

}
