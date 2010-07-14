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
package org.jumpmind.symmetric.service.impl;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jumpmind.symmetric.common.logging.NoOpLog;
import org.jumpmind.symmetric.transport.BandwidthTestResults;
import org.jumpmind.symmetric.web.BandwidthSamplerServlet;
import org.junit.Assert;
import org.junit.Test;

public class BandwidthServiceUnitTest {

    @Test
    public void testDownloadKbps() throws Exception {

        BandwidthService service = new BandwidthService();
        service.log = new NoOpLog();
        int port = 9768;
        BandwidthSamplerServlet servlet = new BandwidthSamplerServlet();
        Server server = startServer(port, "", servlet);
        BandwidthTestResults bw1 = service.getDownloadResultsFor(String.format(
                "http://localhost:%s", port), 1000, 2000);
        Assert.assertTrue(Double.toString(bw1.getKbps()), bw1.getKbps() > 0);
        Assert.assertTrue(Double.toString(bw1.getElapsed()), bw1.getElapsed() > 0);

        servlet.setDefaultTestSlowBandwidthDelay(5);
        BandwidthTestResults bw2 = service.getDownloadResultsFor(String.format(
                "http://localhost:%s", port), 1000, 2000);
        Assert.assertTrue(bw2.getKbps() < bw1.getKbps());
        server.stop();

        Assert.assertEquals(-1d, service.getDownloadKbpsFor(String.format("http://localhost:%s",
                port), 1000, 2000), 0);

    }

    protected Server startServer(int port, String home, BandwidthSamplerServlet servlet)
            throws Exception {
        org.eclipse.jetty.server.Server server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);

        ServletContextHandler webContext = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        webContext.setContextPath(home);
        server.setHandler(webContext);

        ServletHolder servletHolder = new ServletHolder(servlet);
        servletHolder.setInitOrder(0);
        webContext.addServlet(servletHolder, "/bandwidth/*");

        server.start();
        return server;
    }
    
}
