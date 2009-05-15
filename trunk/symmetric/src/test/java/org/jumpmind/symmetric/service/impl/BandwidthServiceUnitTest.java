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

import org.jumpmind.symmetric.transport.BandwidthTestResults;
import org.jumpmind.symmetric.web.BandwidthSamplerServlet;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class BandwidthServiceUnitTest {

    @Test
    public void testDownloadKbps() throws Exception {
        
        BandwidthService service = new BandwidthService();
        int port = 9768;
        BandwidthSamplerServlet servlet = new BandwidthSamplerServlet();
        servlet.setDefaultTestSlowBandwidthDelay(25);
        Server server = startServer(port, "", servlet);
        BandwidthTestResults bw1 = service.getDownloadResultsFor(String.format("http://localhost:%s", port), 100, 2000);
        Assert.assertTrue(Double.toString(bw1.getKbps()), bw1.getKbps() > 0);
        Assert.assertTrue(Double.toString(bw1.getElapsed()), bw1.getElapsed() > 0);
        
        servlet.setDefaultTestSlowBandwidthDelay(50);
        BandwidthTestResults bw2 = service.getDownloadResultsFor(String.format("http://localhost:%s", port), 100, 2000);
        Assert.assertTrue(bw2.getKbps() < bw1.getKbps());
        server.stop();
        
    }
    
    protected Server startServer (int port, String home, BandwidthSamplerServlet servlet) throws Exception {
        Server server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);

        Context webContext = new Context(server, home, Context.NO_SESSIONS);

        ServletHolder servletHolder = new ServletHolder(servlet);
        servletHolder.setInitOrder(0);
        webContext.addServlet(servletHolder, "/bandwidth/*");

        server.addHandler(webContext);
        server.start();
        return server;
    }
}
