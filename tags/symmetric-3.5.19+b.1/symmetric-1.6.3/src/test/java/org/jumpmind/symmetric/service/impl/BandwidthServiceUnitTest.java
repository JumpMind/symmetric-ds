package org.jumpmind.symmetric.service.impl;

import org.apache.commons.logging.impl.NoOpLog;
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
        service.logger = new NoOpLog();
        int port = 9768;
        BandwidthSamplerServlet servlet = new BandwidthSamplerServlet();
        Server server = startServer(port, "", servlet);
        BandwidthTestResults bw1 = service.getDownloadResultsFor(String.format("http://localhost:%s", port), 1000, 2000);
        Assert.assertTrue(Double.toString(bw1.getKbps()), bw1.getKbps() > 0);
        Assert.assertTrue(Double.toString(bw1.getElapsed()), bw1.getElapsed() > 0);
        
        servlet.setDefaultTestSlowBandwidthDelay(5);
        BandwidthTestResults bw2 = service.getDownloadResultsFor(String.format("http://localhost:%s", port), 1000, 2000);
        Assert.assertTrue(bw2.getKbps() < bw1.getKbps());
        server.stop();

        Assert.assertEquals(-1d, service.getDownloadKbpsFor(String.format("http://localhost:%s", port), 1000, 2000), 0);

        
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
