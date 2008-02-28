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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.web.AckServlet;
import org.jumpmind.symmetric.web.AuthenticationFilter;
import org.jumpmind.symmetric.web.PullServlet;
import org.jumpmind.symmetric.web.PushServlet;
import org.jumpmind.symmetric.web.RegistrationServlet;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;

/**
 * Start up SymmetricDS through an embedded Jetty instance.
 * @see SymmetricLauncher#main(String[])
 */
public class SymmetricWebServer {
    
    protected static final Log logger = LogFactory.getLog(SymmetricWebServer.class);

    public void start(int port) throws Exception {

        Server server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.setConnectors(new Connector[] { connector });

        Context webContext = new Context(server, "/sync", Context.NO_SESSIONS);

        webContext.addEventListener(new SymmetricEngineContextLoaderListener());

        webContext.addFilter(AuthenticationFilter.class, "/pull/*", 0);
        webContext.addFilter(AuthenticationFilter.class, "/push/*", 0);
        webContext.addFilter(AuthenticationFilter.class, "/ack/*", 0);

        webContext.addServlet(PullServlet.class, "/pull/*");

        webContext.addServlet(PushServlet.class, "/push/*");

        webContext.addServlet(AckServlet.class, "/ack/*");

        webContext.addServlet(RegistrationServlet.class, "/registration/*");

        server.addHandler(webContext);

        logger.info("About to start SymmetricDS web server on port " + port);
        server.start();
        server.join();
    }

    public static void main(String[] args) throws Exception {
        new SymmetricWebServer().start(8080);
    }

}
