/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
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
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.web.ServletUtils;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start up SymmetricDS through an embedded Jetty instance.
 * 
 * @see SymmetricLauncher#main(String[])
 */
public class SymmetricWebServer {

    protected static final Logger log = LoggerFactory.getLogger(SymmetricWebServer.class);

    protected static final String DEFAULT_WEBAPP_DIR = System.getProperty(
            Constants.SYS_PROP_WEB_DIR, "../web");

    public static final String DEFAULT_HTTP_PORT = System.getProperty(
            Constants.SYS_PROP_DEFAULT_HTTP_PORT, "31415");

    public static final String DEFAULT_HTTPS_PORT = System.getProperty(
            Constants.SYS_PROP_DEFAULT_HTTPS_PORT, "31417");

    public static final int DEFAULT_MAX_IDLE_TIME = 7200000;

    /**
     * The type of HTTP connection to create for this SymmetricDS web server
     */
    public enum Mode {
        HTTP, HTTPS, MIXED;
    }

    private Server server;

    private WebAppContext webapp;

    protected boolean join = true;

    protected boolean createJmxServer = Boolean.parseBoolean(System.getProperty(
            Constants.SYS_PROP_CREATE_JMX_SERVER, "true"));

    protected String webHome = "/";

    protected int maxIdleTime = DEFAULT_MAX_IDLE_TIME;

    protected int httpPort = -1;

    protected int httpsPort = -1;

    protected String basicAuthUsername = null;

    protected String basicAuthPassword = null;

    protected String propertiesFile = null;

    protected String host = null;

    protected boolean noNio = false;

    protected boolean noDirectBuffer = false;

    protected String webAppDir = DEFAULT_WEBAPP_DIR;

    protected String name = "SymmetricDS";

    public SymmetricWebServer() {
    }

    public SymmetricWebServer(String propertiesUrl) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
    }

    public SymmetricWebServer(String propertiesUrl, String webappDir) {
        this.propertiesFile = propertiesUrl;
        this.webAppDir = webappDir;
    }

    public SymmetricWebServer(String webDirectory, int maxIdleTime, String propertiesUrl,
            boolean join, boolean noNio, boolean noDirectBuffer) {
        this(propertiesUrl, webDirectory);
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
        this.propertiesFile = propertiesUrl;
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
            throw new IllegalStateException(
                    "Either an http or https port needs to be set before starting the server.");
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

        // indicate to the app that we are in standalone mode
        System.setProperty(Constants.SYS_PROP_STANDALONE_WEB, "true");

        server = new Server();

        server.setConnectors(getConnectors(port, securePort, mode));
        setupBasicAuthIfNeeded(server);

        webapp = new WebAppContext();
        webapp.setParentLoaderPriority(true);
        webapp.setContextPath(webHome);
        webapp.setWar(webAppDir);
        webapp.getSessionHandler().getSessionManager().setMaxInactiveInterval(maxIdleTime / 1000);
        if (propertiesFile != null) {
            webapp.getServletContext().setInitParameter(
                    WebConstants.INIT_SINGLE_SERVER_PROPERTIES_FILE, propertiesFile);
            webapp.getServletContext().setInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE,
                    Boolean.toString(false));            
        } else {
            webapp.getServletContext().setInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE,
                    Boolean.toString(true));

        }
        server.setHandler(webapp);

        server.start();

        if (createJmxServer) {
            int httpJmxPort = port != 0 ? port + 1 : securePort + 1;
            registerHttpJmxAdaptor(httpJmxPort);
        }

        if (join) {
            log.info("Joining the web server main thread");
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
            SymmetricEngineHolder engineHolder = ServletUtils
                    .getSymmetricEngineHolder(servletContext);
            if (engineHolder != null) {
                if (engineHolder.getEngines().size() == 1) {
                    return engineHolder.getEngines().values().iterator().next();
                } else {
                    throw new IllegalStateException(
                            "Could not choose a single engine to return.  There are "
                                    + engineHolder.getEngines().size() + " engines configured.");
                }
            }
        }
        return engine;
    }

    public void waitForEnginesToComeOnline(long maxWaitTimeInMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        ServletContext servletContext = getServletContext();
        if (servletContext != null) {
            SymmetricEngineHolder engineHolder = ServletUtils
                    .getSymmetricEngineHolder(servletContext);
            while (engineHolder.areEnginesStarting()) {
                AppUtils.sleep(500);
                if ((System.currentTimeMillis() - startTime) > maxWaitTimeInMs) {
                    throw new InterruptedException("Timed out waiting for engines to start");
                }
            }
        }
    }

    protected void setupBasicAuthIfNeeded(Server server) {
        if (StringUtils.isNotBlank(basicAuthUsername)) {
            ConstraintSecurityHandler sh = new ConstraintSecurityHandler();

            Constraint constraint = new Constraint();
            constraint.setName(Constraint.__BASIC_AUTH);
            ;
            constraint.setRoles(new String[] { SecurityConstants.EMBEDDED_WEBSERVER_DEFAULT_ROLE });
            constraint.setAuthenticate(true);

            ConstraintMapping cm = new ConstraintMapping();
            cm.setConstraint(constraint);
            cm.setPathSpec("/*");
            // sh.setConstraintMappings(new ConstraintMapping[] {cm});
            sh.addConstraintMapping(cm);

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
        String keyStoreType = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_TYPE);

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
            log.info("About to start {} web server on port {}", name, port);
        }
        if (mode.equals(Mode.HTTPS) || mode.equals(Mode.MIXED)) {
            Connector connector = new SslSocketConnector();
            String keyStorePassword = System
                    .getProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD);
            keyStorePassword = (keyStorePassword != null) ? keyStorePassword
                    : SecurityConstants.KEYSTORE_PASSWORD;
            ((SslSocketConnector) connector).setKeystore(keyStoreFile);
            ((SslSocketConnector) connector).setKeyPassword(keyStorePassword);
            if (keyStoreType != null) {
                ((SslSocketConnector) connector).setKeystoreType(keyStoreType);
            }

            ((SslSocketConnector) connector).setMaxIdleTime(maxIdleTime);
            connector.setPort(securePort);
            connector.setHost(host);
            connectors.add(connector);
            log.info("About to start SymmetricDS web server on secure port {}", securePort);
        }
        return connectors.toArray(new Connector[connectors.size()]);
    }

    protected void registerHttpJmxAdaptor(int jmxPort) throws Exception {
        if (AppUtils.isSystemPropertySet(SystemConstants.JMX_HTTP_CONSOLE_ENABLED, true)) {
            log.info("Starting JMX HTTP console on port {}", jmxPort);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = getHttpJmxAdaptorName();
            mbeanServer.createMBean(HttpAdaptor.class.getName(), name);
            if (!AppUtils.isSystemPropertySet(SystemConstants.JMX_HTTP_CONSOLE_LOCALHOST_ENABLED,
                    true)) {
                mbeanServer.setAttribute(name, new Attribute("Host", "0.0.0.0"));
            } else if (StringUtils.isNotBlank(host)) {
                mbeanServer.setAttribute(name, new Attribute("Host", host));
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
                log.warn("Could not unregister the JMX HTTP Adaptor");
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

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
    }

    public int getHttpsPort() {
        return httpsPort;
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

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
