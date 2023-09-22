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

import org.eclipse.jetty.server.AbstractConnector;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.common.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

import jakarta.servlet.ServletContext;

@Component
public class JettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory>, JettyServerCustomizer {
    private static final Logger log = LoggerFactory.getLogger(JettyCustomizer.class);
    protected WebAppContext webapp;

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        factory.addServerCustomizers(this);
    }

    @Override
    public void customize(Server server) {
        TypedProperties sysProps = new TypedProperties(System.getProperties());
        boolean httpEnabled = sysProps.is(ServerConstants.HTTP_ENABLE, true);
        boolean httpsEnabled = sysProps.is(ServerConstants.HTTPS_ENABLE, false);
        String ignoredProtocols = System.getProperty(SecurityConstants.SYSPROP_SSL_IGNORE_PROTOCOLS, "SSLv3");
        String ignoredCiphers = System.getProperty(SecurityConstants.SYSPROP_SSL_IGNORE_CIPHERS);
        if (httpsEnabled) {
            for (Connector connector : server.getConnectors()) {
                if (connector instanceof ServerConnector serverConnector) {
                    HttpConnectionFactory connectionFactory = serverConnector.getConnectionFactory(HttpConnectionFactory.class);
                    if (connectionFactory != null) {
                        HttpConfiguration httpsConfig = connectionFactory.getHttpConfiguration();
                        if (httpsConfig != null) {
                            httpsConfig.setSendServerVersion(false);
                            SecureRequestCustomizer customizer = httpsConfig.getCustomizer(SecureRequestCustomizer.class);
                            if (customizer != null) {
                                if (Boolean.parseBoolean(System.getProperty(ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS, Boolean.toString(true)))) {
                                    customizer.setSniHostCheck(false);
                                }
                            }
                        }
                    }
                }
                if (connector instanceof AbstractConnector) {
                    for (ConnectionFactory connectionFactory : ((AbstractConnector) connector).getConnectionFactories()) {
                        if (connectionFactory instanceof SslConnectionFactory) {
                            SslContextFactory sslContextFactory = ((SslConnectionFactory) connectionFactory).getSslContextFactory();
                            if (ignoredProtocols != null && ignoredProtocols.length() > 0) {
                                String[] protocols = ignoredProtocols.split(",");
                                sslContextFactory.addExcludeProtocols(protocols);
                            }
                            if (ignoredCiphers != null && ignoredCiphers.length() > 0) {
                                String[] ciphers = ignoredCiphers.split(",");
                                sslContextFactory.addExcludeCipherSuites(ciphers);
                            }
                        }
                    }
                }
            }
        }
        if (httpEnabled && httpsEnabled) {
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSendServerVersion(false);
            ServerConnector connector = new ServerConnector(server);
            connector.addConnectionFactory(new HttpConnectionFactory(httpConfig));
            connector.setPort(sysProps.getInt(ServerConstants.HTTP_PORT, Integer.parseInt(SymmetricWebServer.DEFAULT_HTTP_PORT)));
            server.addConnector(connector);
        }
        for (Handler handler : server.getHandlers()) {
            if (handler instanceof WebAppContext webapp) {
                this.webapp = webapp;
                webapp.setParentLoaderPriority(true);
            }
        }
        Class<?> remoteStatusEndpoint = loadRemoteStatusEndpoint();
        if (remoteStatusEndpoint != null) {
            // TODO: get websockets to work
            /*
             * ServletContextHandler handler = new ServletContextHandler(server, "/control"); JakartaWebSocketServletContainerInitializer.configure(handler,
             * (servletContext, container) -> { container.setDefaultMaxBinaryMessageBufferSize(Integer.MAX_VALUE);
             * container.setDefaultMaxTextMessageBufferSize(Integer.MAX_VALUE); //container.addEndpoint(remoteStatusEndpoint);
             * container.addEndpoint(ServerEndpointConfig.Builder.create(remoteStatusEndpoint, "/control") .subprotocols(List.of("my-ws-protocol")).build());
             * }); server.setHandler(handler);
             */
            if (sysProps.is(ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS)) {
                System.setProperty("org.eclipse.jetty.websocket.jsr356.ssl-trust-all", Boolean.toString(true));
            }
        }
    }

    protected Class<?> loadRemoteStatusEndpoint() {
        try {
            Class<?> clazz = Class.forName("com.jumpmind.symmetric.console.remote.ServerEndpoint");
            return clazz;
        } catch (ClassNotFoundException ex) {
            // ServerEndpoint not found. This is an expected condition.
        } catch (Exception ex) {
            log.debug("Failed to load remote status endpoint.", ex);
        }
        return null;
    }

    public ServletContext getServletContext() {
        return webapp != null ? webapp.getServletContext() : null;
    }
}