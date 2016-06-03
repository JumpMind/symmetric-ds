/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.servlet.DispatcherType;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.session.AbstractSession;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.HashedSession;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.common.ServerConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
import org.jumpmind.symmetric.web.HttpMethodFilter;
import org.jumpmind.symmetric.web.ServletUtils;
import org.jumpmind.symmetric.web.SymmetricEngineHolder;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.symmetric.web.rest.RestService;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import mx4j.tools.adaptor.http.HttpAdaptor;
import mx4j.tools.adaptor.http.XSLTProcessor;

/**
 * Start up SymmetricDS through an embedded Jetty instance.
 *
 * @see SymmetricLauncher#main(String[])
 */
public class SymmetricWebServer {

    protected static final Logger log = LoggerFactory.getLogger(SymmetricWebServer.class);

    protected static final String DEFAULT_WEBAPP_DIR = System.getProperty(SystemConstants.SYSPROP_WEB_DIR, AppUtils.getSymHome() + "/web");

    public static final String DEFAULT_HTTP_PORT = System.getProperty(SystemConstants.SYSPROP_DEFAULT_HTTP_PORT, "31415");

    public static final String DEFAULT_JMX_PORT = System.getProperty(SystemConstants.SYSPROP_DEFAULT_JMX_PORT, "31416");

    public static final String DEFAULT_HTTPS_PORT = System.getProperty(SystemConstants.SYSPROP_DEFAULT_HTTPS_PORT, "31417");

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

    protected String webHome = "/";

    protected int maxIdleTime = DEFAULT_MAX_IDLE_TIME;

    protected boolean httpEnabled = true;

    protected int httpPort = Integer.parseInt(DEFAULT_HTTP_PORT);

    protected boolean httpsEnabled = false;

    protected int httpsPort = -1;

    protected boolean jmxEnabled = true;

    protected int jmxPort = Integer.parseInt(DEFAULT_JMX_PORT);

    protected String basicAuthUsername = null;

    protected String basicAuthPassword = null;

    protected String propertiesFile = null;

    protected String host = null;

    protected boolean noNio = false;

    protected boolean noDirectBuffer = false;

    protected String webAppDir = DEFAULT_WEBAPP_DIR;

    protected String name = "SymmetricDS";

    protected String httpSslVerifiedServerNames = "all";

    protected String allowDirListing = "false";
    
    protected String disallowedMethods = "OPTIONS";
    
    protected String allowedMethods = "";

    protected boolean allowSelfSignedCerts = true;

    public SymmetricWebServer() {
        this(null, DEFAULT_WEBAPP_DIR);
    }

    public SymmetricWebServer(String propertiesUrl) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
    }

    public SymmetricWebServer(int maxIdleTime, String propertiesUrl) {
        this(propertiesUrl, DEFAULT_WEBAPP_DIR);
        this.maxIdleTime = maxIdleTime;
    }

    public SymmetricWebServer(String webDirectory, int maxIdleTime, String propertiesUrl, boolean join, boolean noNio, boolean noDirectBuffer) {
        this(propertiesUrl, webDirectory);
        this.maxIdleTime = maxIdleTime;
        this.join = join;
        this.noDirectBuffer = noDirectBuffer;
        this.noNio = noNio;
    }

    public SymmetricWebServer(String propertiesUrl, String webappDir) {
        this.propertiesFile = propertiesUrl;
        this.webAppDir = webappDir;
        initFromProperties();
    }

    protected void initFromProperties() {

        try {
            Class.forName(AbstractCommandLauncher.class.getName());
        } catch (ClassNotFoundException e) {
        }

        TypedProperties serverProperties = new TypedProperties(System.getProperties());
        httpEnabled = serverProperties.is(ServerConstants.HTTP_ENABLE,
                Boolean.parseBoolean(System.getProperty(ServerConstants.HTTP_ENABLE, "true")));
        httpsEnabled = serverProperties.is(ServerConstants.HTTPS_ENABLE,
                Boolean.parseBoolean(System.getProperty(ServerConstants.HTTPS_ENABLE, "true")));
        jmxEnabled = serverProperties.is(ServerConstants.JMX_HTTP_ENABLE,
                Boolean.parseBoolean(System.getProperty(ServerConstants.JMX_HTTP_ENABLE, "true")));
        httpPort = serverProperties.getInt(ServerConstants.HTTP_PORT,
                Integer.parseInt(System.getProperty(ServerConstants.HTTP_PORT, "" + httpPort)));
        httpsPort = serverProperties.getInt(ServerConstants.HTTPS_PORT,
                Integer.parseInt(System.getProperty(ServerConstants.HTTPS_PORT, "" + httpsPort)));
        jmxPort = serverProperties.getInt(ServerConstants.JMX_HTTP_PORT,
                Integer.parseInt(System.getProperty(ServerConstants.JMX_HTTP_PORT, "" + jmxPort)));
        host = serverProperties.get(ServerConstants.HOST_BIND_NAME, System.getProperty(ServerConstants.HOST_BIND_NAME, host));
        httpSslVerifiedServerNames = serverProperties.get(ServerConstants.HTTPS_VERIFIED_SERVERS,
                System.getProperty(ServerConstants.HTTPS_VERIFIED_SERVERS, httpSslVerifiedServerNames));
        allowSelfSignedCerts = serverProperties.is(ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS,
                Boolean.parseBoolean(System.getProperty(ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS, "" + allowSelfSignedCerts)));
        allowDirListing = serverProperties.get(ServerConstants.SERVER_ALLOW_DIR_LISTING, "false");
        allowedMethods = serverProperties.get(ServerConstants.SERVER_ALLOW_HTTP_METHODS, "");
        disallowedMethods = serverProperties.get(ServerConstants.SERVER_DISALLOW_HTTP_METHODS, "OPTIONS");
    }

    public SymmetricWebServer start(int httpPort, int jmxPort, String propertiesUrl) throws Exception {
        this.propertiesFile = propertiesUrl;
        return start(httpPort, jmxPort);
    }

    public SymmetricWebServer start() throws Exception {
        if (httpPort > 0 && httpsPort > 0 && httpEnabled && httpsEnabled) {
            return startMixed(httpPort, httpsPort, jmxPort);
        } else if (httpPort > 0 && httpEnabled) {
            return start(httpPort, jmxPort);
        } else if (httpsPort > 0 && httpsEnabled) {
            return startSecure(httpsPort, jmxPort);
        } else {
            throw new IllegalStateException("Either an http or https port needs to be set before starting the server.");
        }
    }

    public SymmetricWebServer start(int httpPort) throws Exception {
        return start(httpPort, 0, httpPort + 1, Mode.HTTP);
    }

    public SymmetricWebServer start(int httpPort, int jmxPort) throws Exception {
        return start(httpPort, 0, jmxPort, Mode.HTTP);
    }

    public SymmetricWebServer startSecure(int httpsPort, int jmxPort) throws Exception {
        return start(0, httpsPort, jmxPort, Mode.HTTPS);
    }

    public SymmetricWebServer startMixed(int httpPort, int secureHttpPort, int jmxPort) throws Exception {
        return start(httpPort, secureHttpPort, jmxPort, Mode.MIXED);
    }

    public SymmetricWebServer start(int httpPort, int securePort, int httpJmxPort, Mode mode) throws Exception {

        TransportManagerFactory.initHttps(httpSslVerifiedServerNames, allowSelfSignedCerts);

        // indicate to the app that we are in stand alone mode
        System.setProperty(SystemConstants.SYSPROP_STANDALONE_WEB, "true");

        server = new Server();

        server.setConnectors(getConnectors(server, httpPort, securePort, mode));
        setupBasicAuthIfNeeded(server);

        webapp = new WebAppContext();
        webapp.setParentLoaderPriority(true);
        webapp.setConfigurationDiscovered(true);
        webapp.setContextPath(webHome);
        webapp.setWar(webAppDir);
        webapp.setResourceBase(webAppDir);

        webapp.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", allowDirListing);

        SessionManager sm = new SessionManager();
        webapp.getSessionHandler().setSessionManager(sm);

        FilterHolder filterHolder = new FilterHolder(HttpMethodFilter.class);
        filterHolder.setInitParameter("server.allow.http.methods", allowedMethods);
        filterHolder.setInitParameter("server.disallow.http.methods", disallowedMethods);
        webapp.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));
        
        webapp.getServletContext().getContextHandler()
                .setMaxFormContentSize(Integer.parseInt(System.getProperty("org.eclipse.jetty.server.Request.maxFormContentSize", "800000")));
        webapp.getServletContext().getContextHandler()
                .setMaxFormKeys(Integer.parseInt(System.getProperty("org.eclipse.jetty.server.Request.maxFormKeys", "100000")));
        if (propertiesFile != null) {
            webapp.getServletContext().getContextHandler().setInitParameter(WebConstants.INIT_SINGLE_SERVER_PROPERTIES_FILE, propertiesFile);
            webapp.getServletContext().getContextHandler()
                    .setInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE, Boolean.toString(false));
        } else {
            webapp.getServletContext().getContextHandler()
                    .setInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE, Boolean.toString(true));
        }
        server.setHandler(webapp);

        server.start();

        if (httpJmxPort > 0) {
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

    public RestService getRestService() {
        ServletContext servletContext = getServletContext();
        WebApplicationContext rootContext = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        return rootContext.getBean(RestService.class);
    }

    public ISymmetricEngine getEngine() {
        ISymmetricEngine engine = null;
        ServletContext servletContext = getServletContext();
        if (servletContext != null) {
            SymmetricEngineHolder engineHolder = ServletUtils.getSymmetricEngineHolder(servletContext);
            if (engineHolder != null) {
                if (engineHolder.getEngines().size() == 1) {
                    return engineHolder.getEngines().values().iterator().next();
                } else {
                    throw new IllegalStateException("Could not choose a single engine to return.  There are "
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
            SymmetricEngineHolder engineHolder = ServletUtils.getSymmetricEngineHolder(servletContext);
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

    protected Connector[] getConnectors(Server server, int port, int securePort, Mode mode) {
        ArrayList<Connector> connectors = new ArrayList<Connector>();

        HttpConfiguration httpConfig = new HttpConfiguration();
        if (mode.equals(Mode.HTTPS) || mode.equals(Mode.MIXED)) {
            httpConfig.setSecureScheme("https");
            httpConfig.setSecurePort(securePort);
        }

        httpConfig.setOutputBufferSize(32768);

        if (mode.equals(Mode.HTTP) || mode.equals(Mode.MIXED)) {
            ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
            http.setPort(port);
            http.setHost(host);
            http.setIdleTimeout(maxIdleTime);
            connectors.add(http);
            log.info(String.format("About to start %s web server on host:port %s:%s", name, host == null ? "default" : host, port));
        }
        if (mode.equals(Mode.HTTPS) || mode.equals(Mode.MIXED)) {
            ISecurityService securityService = SecurityServiceFactory.create(SecurityServiceType.SERVER,
                    new TypedProperties(System.getProperties()));
            securityService.installDefaultSslCert(host);
            String keyStorePassword = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD);
            keyStorePassword = (keyStorePassword != null) ? keyStorePassword : SecurityConstants.KEYSTORE_PASSWORD;
            SslContextFactory sslConnectorFactory = new SslContextFactory();
            sslConnectorFactory.setKeyManagerPassword(keyStorePassword);
            /* Prevent POODLE attack */
            String ignoredProtocols = System.getProperty(SecurityConstants.SYSPROP_SSL_IGNORE_PROTOCOLS);
            if (ignoredProtocols != null && ignoredProtocols.length() > 0) {
                String[] protocols = ignoredProtocols.split(",");
                sslConnectorFactory.addExcludeProtocols(protocols);
            }
            else {
                sslConnectorFactory.addExcludeProtocols("SSLv3");
            }

            String ignoredCiphers = System.getProperty(SecurityConstants.SYSPROP_SSL_IGNORE_CIPHERS);
            if (ignoredCiphers != null && ignoredCiphers.length() > 0) {
                String[] ciphers = ignoredCiphers.split(",");
                sslConnectorFactory.addExcludeCipherSuites(ciphers);
            }

            sslConnectorFactory.setCertAlias(System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_CERT_ALIAS,
                    SecurityConstants.ALIAS_SYM_PRIVATE_KEY));
            sslConnectorFactory.setKeyStore(securityService.getKeyStore());
            sslConnectorFactory.setTrustStore(securityService.getTrustStore());

            
            HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
            httpsConfig.addCustomizer(new SecureRequestCustomizer());

            ServerConnector https = new ServerConnector(server,
                    new SslConnectionFactory(sslConnectorFactory, HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(httpsConfig));
            https.setPort(securePort);
            https.setIdleTimeout(maxIdleTime);
            https.setHost(host);
            connectors.add(https);
            log.info(String.format("About to start %s web server on secure host:port %s:%s", name, host == null ? "default" : host,
                    securePort));
        }
        return connectors.toArray(new Connector[connectors.size()]);
    }

    protected void registerHttpJmxAdaptor(int jmxPort) throws Exception {
        if (AppUtils.isSystemPropertySet(SystemConstants.SYSPROP_JMX_HTTP_CONSOLE_ENABLED, true) && jmxEnabled) {
            log.info("Starting JMX HTTP console on port {}", jmxPort);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = getHttpJmxAdaptorName();
            mbeanServer.createMBean(HttpAdaptor.class.getName(), name);
            if (!AppUtils.isSystemPropertySet(SystemConstants.SYSPROP_JMX_HTTP_CONSOLE_LOCALHOST_ENABLED, true)) {
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
        if (AppUtils.isSystemPropertySet(SystemConstants.SYSPROP_JMX_HTTP_CONSOLE_ENABLED, true) && jmxEnabled) {
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
            removeHttpJmxAdaptor();
            server.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        new SymmetricWebServer().start(8080, 8081);
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
        System.setProperty(ServerConstants.HTTP_PORT, Integer.toString(httpPort));
        this.httpPort = httpPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpsPort(int httpsPort) {
        System.setProperty(ServerConstants.HTTPS_PORT, Integer.toString(httpsPort));
        this.httpsPort = httpsPort;
    }

    public int getHttpsPort() {
        return httpsPort;
    }

    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
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

    public boolean isNoNio() {
        return noNio;
    }

    public void setNoDirectBuffer(boolean noDirectBuffer) {
        this.noDirectBuffer = noDirectBuffer;
    }

    public boolean isNoDirectBuffer() {
        return noDirectBuffer;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public void setHttpEnabled(boolean httpEnabled) {
        this.httpEnabled = httpEnabled;
    }

    public boolean isHttpEnabled() {
        return httpEnabled;
    }

    public void setHttpsEnabled(boolean httpsEnabled) {
        this.httpsEnabled = httpsEnabled;
    }

    public boolean isHttpsEnabled() {
        return httpsEnabled;
    }

    public void setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
    }

    public boolean isJmxEnabled() {
        return jmxEnabled;
    }

    class SessionManager extends HashSessionManager {

        public SessionManager() {
            setMaxInactiveInterval(10 * 60);
            setLazyLoad(true);
            setDeleteUnrestorableSessions(true);
            setSessionCookie(getSessionCookie() + (httpPort > 0 ? httpPort
                    : httpsPort));
        }

        @Override
        protected AbstractSession newSession(HttpServletRequest request) {
            return new Session(this, request);
        }

        @Override
        protected AbstractSession newSession(long created, long accessed, String clusterId) {
            return new Session(this, created, accessed, clusterId);
        }

        @Override
        protected synchronized HashedSession restoreSession(String idInCuster) {
            if (isNotBlank(idInCuster)) {
                return super.restoreSession(idInCuster);
            } else {
                return null;
            }
        }

        public HashedSession restoreSession(InputStream is, HashedSession session) throws Exception {
            DataInputStream di = new DataInputStream(is);

            String clusterId = di.readUTF();
            di.readUTF(); // nodeId

            long created = di.readLong();
            long accessed = di.readLong();
            int requests = di.readInt();

            if (session == null)
                session = (HashedSession) newSession(created, accessed, clusterId);
            session.setRequests(requests);

            int size = di.readInt();

            restoreSessionAttributes(di, size, session);

            try {
                int maxIdle = di.readInt();
                session.setMaxInactiveInterval(maxIdle);
            } catch (EOFException e) {
                log.debug("No maxInactiveInterval persisted for session " + clusterId, e);
            }

            return session;
        }

        private void restoreSessionAttributes(InputStream is, int size, HashedSession session) throws Exception {
            if (size > 0) {
                ObjectInputStream ois = new ObjectInputStream(is);
                for (int i = 0; i < size; i++) {
                    String key = ois.readUTF();
                    try {
                        Object value = ois.readObject();
                        session.setAttribute(key, value);
                    } catch (Exception ex) {
                        if (ex instanceof ClassCastException || ex instanceof ClassNotFoundException) {
                            log.warn("Could not restore the '" + key
                                    + "' session object.  Code has probably changed.  The error message was: " + ex.getMessage());
                        } else {
                            log.error("Could not restore the '" + key + "' session object.", ex);
                        }
                    }
                }
            }
        }

    }

    class Session extends HashedSession {

        protected Session(HashSessionManager hashSessionManager, HttpServletRequest request) {
            super(hashSessionManager, request);
        }

        protected Session(HashSessionManager hashSessionManager, long created, long accessed, String clusterId) {
            super(hashSessionManager, created, accessed, clusterId);
        }

        @Override
        public synchronized void save(OutputStream os) throws IOException {
            DataOutputStream out = new DataOutputStream(os);
            out.writeUTF(getClusterId());
            out.writeUTF(getNodeId());
            out.writeLong(getCreationTime());
            out.writeLong(getAccessed());
            out.writeInt(getRequests());

            Enumeration<String> e = getAttributeNames();
            int count = 0;
            while (e.hasMoreElements()) {
                String key = e.nextElement();
                Object obj = doGet(key);
                if (obj instanceof Serializable) {
                    count++;
                }
            }
            out.writeInt(count);
            ObjectOutputStream oos = new ObjectOutputStream(out);
            e = getAttributeNames();
            while (e.hasMoreElements()) {
                String key = e.nextElement();
                Object obj = doGet(key);
                if (obj instanceof Serializable) {
                    oos.writeUTF(key);
                    oos.writeObject(obj);
                }
            }
            oos.flush();
            out.writeInt(getMaxInactiveInterval());
        }

    }

}
