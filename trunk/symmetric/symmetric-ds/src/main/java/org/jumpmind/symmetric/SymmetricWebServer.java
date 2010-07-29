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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.servlet.ServletContext;

import mx4j.tools.adaptor.http.HttpAdaptor;
import mx4j.tools.adaptor.http.XSLTProcessor;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.http.security.Constraint;
import org.eclipse.jetty.http.security.Password;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Start up SymmetricDS through an embedded Jetty instance.
 * 
 * @see SymmetricLauncher#main(String[])
 */
public class SymmetricWebServer {

    protected static final ILog log = LogFactory.getLog(SymmetricWebServer.class);
    
    protected static final String DEFAULT_WEBAPP_DIR = "../web";

    /**
     * The type of HTTP connection to create for this SymmetricDS web server
     */
    public enum Mode {
        HTTP, HTTPS, MIXED;
    }

    private Server server;
    
    private WebAppContext webapp;

    protected boolean join = true;

    protected boolean createJmxServer = true;

    protected String webHome = "/sync";

    protected int maxIdleTime = 900000;

    protected int httpPort = -1;

    protected int httpsPort = -1;
    
    protected String basicAuthUsername = null;
    
    protected String basicAuthPassword = null;

    protected String propertiesFile = null;

    protected String host = null;
    
    protected boolean noNio = false;
    
    protected boolean noDirectBuffer = false;
    
    protected String webAppDir = DEFAULT_WEBAPP_DIR;    

    public SymmetricWebServer() {
    }
    
    public SymmetricWebServer(String propertiesUrl) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
    }

    public SymmetricWebServer(String propertiesUrl, String webappDir) {
        this.propertiesFile = propertiesUrl ;
        this.webAppDir = webappDir;
    }

    public SymmetricWebServer(int maxIdleTime, String propertiesUrl, boolean join, boolean noNio, boolean noDirectBuffer) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
        this.maxIdleTime = maxIdleTime;
        this.join = join;
        this.noDirectBuffer = noDirectBuffer;
        this.noNio = noNio;
    }

    public SymmetricWebServer(int maxIdleTime, String propertiesUrl) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
        this.maxIdleTime = maxIdleTime;
    }

    public SymmetricWebServer start(int port, String propertiesUrl) throws Exception {
        this.propertiesFile = propertiesUrl ;
        return start(port);
    }

    public SymmetricWebServer start() throws Exception {
        if (httpPort > 0 && httpsPort > 0) {
            return startMixed(httpPort, httpsPort);
        } else if (httpPort > 0) {
            return start(httpPort);
        } else if (httpsPort > 0) {
            return startSecure(httpsPort);
        } else {
            throw new IllegalStateException("Either an http or https port needs to be set before starting the server.");
        }
    }

    public SymmetricWebServer start(int port) throws Exception {
        return start(port, 0, Mode.HTTP);
    }

    public SymmetricWebServer startSecure(int port) throws Exception {
        return start(0, port, Mode.HTTPS);
    }

    public SymmetricWebServer startMixed(int port, int securePort) throws Exception {
        return start(port, securePort, Mode.MIXED);
    }

    public SymmetricWebServer start(int port, int securePort, Mode mode) throws Exception {
        server = new Server();

        server.setConnectors(getConnectors(port, securePort, mode));
        setupBasicAuthIfNeeded(server);

        // Configure JMX for Jetty
        MBeanContainer mbContainer=new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        server.getContainer().addEventListener(mbContainer);
        server.addBean(mbContainer);
        mbContainer.addBean(Log.getLog());
        
        webapp = new WebAppContext();
        webapp.setContextPath(webHome);
        webapp.setWar(webAppDir);
        
        server.setHandler(webapp);

        if (!StringUtils.isBlank(propertiesFile)) {
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, propertiesFile);    
        }
        
        server.start();

        if (createJmxServer) {
            int httpJmxPort = port != 0 ? port + 1 : securePort + 1;
            registerHttpJmxAdaptor(httpJmxPort);
        }

        if (join) {
            server.join();
        }

        return this;
    }
    
    protected ServletContext getServletContext() {
        return webapp != null ? webapp.getServletContext() : null;
    }
    
    public ISymmetricEngine getEngine() {
        ISymmetricEngine engine = null;
        ServletContext servletContext = getServletContext();
        if (servletContext != null) {
           ApplicationContext appContext = WebApplicationContextUtils.getWebApplicationContext(servletContext);
           engine = appContext.getBean(ISymmetricEngine.class);
        }
        return engine;
    }

    protected void setupBasicAuthIfNeeded(Server server) {
        if (StringUtils.isNotBlank(basicAuthUsername) ) {
            ConstraintSecurityHandler sh = new ConstraintSecurityHandler();

            Constraint constraint = new Constraint();
            constraint.setName(Constraint.__BASIC_AUTH);;
            constraint.setRoles(new String[]{SecurityConstants.EMBEDDED_WEBSERVER_DEFAULT_ROLE});
            constraint.setAuthenticate(true);
    
            ConstraintMapping cm = new ConstraintMapping();
            cm.setConstraint(constraint);
            cm.setPathSpec("/*");
            sh.setConstraintMappings(new ConstraintMapping[] {cm});
            //sh.addConstraintMapping(cm);
    
            sh.setAuthenticator(new BasicAuthenticator());
            
            HashLoginService loginService = new HashLoginService();
            loginService.putUser(basicAuthUsername, new Password(basicAuthPassword), null);
            sh.setLoginService(loginService);

            server.setHandler(sh);
            
        }
    }

    protected Connector[] getConnectors(int port, int securePort, Mode mode) {
        ArrayList<Connector> connectors = new ArrayList<Connector>();
        String keyStoreFile = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE);

        if (mode.equals(Mode.HTTP) || mode.equals(Mode.MIXED)) {
            Connector connector = null;
            if (noNio) {
              connector = new SocketConnector();                
            } else {
              SelectChannelConnector nioConnector = new SelectChannelConnector();              
              nioConnector.setUseDirectBuffers(!noDirectBuffer);
              connector = nioConnector;
            }
            connector.setPort(port);
            connector.setHost(host);
            connector.setMaxIdleTime(maxIdleTime);            
            connectors.add(connector);  
            log.info("WebServerStarting", port);
        }
        if (mode.equals(Mode.HTTPS) || mode.equals(Mode.MIXED)) {
            Connector connector = new SslSocketConnector();
            String keyStorePassword = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD);
            keyStorePassword = (keyStorePassword != null) ? keyStorePassword : SecurityConstants.KEYSTORE_PASSWORD;
            ((SslSocketConnector) connector).setKeystore(keyStoreFile);
            ((SslSocketConnector) connector).setKeyPassword(keyStorePassword);
            ((SslSocketConnector) connector).setMaxIdleTime(maxIdleTime);
            connector.setPort(securePort);
            connector.setHost(host);
            connectors.add(connector);
            log.info("WebServerSecureStarting", securePort);
        }
        return connectors.toArray(new Connector[connectors.size()]);
    }

    protected void registerHttpJmxAdaptor(int jmxPort) throws Exception {
        if (AppUtils.isSystemPropertySet(SystemConstants.JMX_HTTP_CONSOLE_ENABLED, true)) {
            log.info("JMXConsoleStarting", jmxPort);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = getHttpJmxAdaptorName();
            mbeanServer.createMBean(HttpAdaptor.class.getName(), name);
            if (!AppUtils.isSystemPropertySet(SystemConstants.JMX_HTTP_CONSOLE_LOCALHOST_ENABLED, true)) {
                mbeanServer.setAttribute(name, new Attribute("Host", "0.0.0.0"));
            }
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
        if (AppUtils.isSystemPropertySet(SystemConstants.JMX_HTTP_CONSOLE_ENABLED, true)) {
            try {
                MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
                mbeanServer.unregisterMBean(getHttpJmxAdaptorName());
                mbeanServer.unregisterMBean(getXslJmxAdaptorName());
            } catch (Exception e) {
                log.warn("JMXAdaptorUnregisterFailed");
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

    public void setCreateJmxServer(boolean createJmxServer) {
        this.createJmxServer = createJmxServer;
    }

    public void setHost(String host) {
        this.host = host;
    }
    
    public void setBasicAuthPassword(String basicAuthPassword) {
        this.basicAuthPassword = basicAuthPassword;
    }

    public void setBasicAuthUsername(String basicAuthUsername) {
        this.basicAuthUsername = basicAuthUsername;
    }
    
    public void setWebAppDir(String webAppDir) {
        this.webAppDir = webAppDir;
    }
    
    public void setNoNio(boolean noNio) {
        this.noNio = noNio;
    }
    
    public void setNoDirectBuffer(boolean noDirectBuffer) {
        this.noDirectBuffer = noDirectBuffer;
    }
}
