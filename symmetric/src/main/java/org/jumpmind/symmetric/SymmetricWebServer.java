package org.jumpmind.symmetric;

import javawebparts.filter.CompressionFilter;

import org.jumpmind.symmetric.web.AckServlet;
import org.jumpmind.symmetric.web.AuthenticationFilter;
import org.jumpmind.symmetric.web.PullServlet;
import org.jumpmind.symmetric.web.PushServlet;
import org.jumpmind.symmetric.web.RegistrationServlet;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;

public class SymmetricWebServer {

    public void start(int port) throws Exception {

        Server server = new Server();
        Connector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.setConnectors(new Connector[] { connector });

        Context webContext = new Context(server, "/sync", Context.NO_SESSIONS);

        webContext.addEventListener(new SymmetricEngineContextLoaderListener());
        FilterHolder compressionFilter = new FilterHolder(CompressionFilter.class);
        compressionFilter.setInitParameter("compressType", "gzip_only");
        webContext.addFilter(compressionFilter, "/*", 0);
        
        webContext.addFilter(AuthenticationFilter.class, "/*", 0);

        webContext.addServlet(PullServlet.class, "/pull/*");

        webContext.addServlet(PushServlet.class, "/push/*");

        webContext.addServlet(AckServlet.class, "/ack/*");

        webContext.addServlet(RegistrationServlet.class, "/registration/*");

        server.addHandler(webContext);
        server.start();
        server.join();
    }

    public static void main(String[] args) throws Exception {
        new SymmetricWebServer().start(8080);
    }

}
