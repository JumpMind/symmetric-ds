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
 * under the License.  */

package org.jumpmind.symmetric.service.impl;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jumpmind.symmetric.common.logging.NoOpLog;
import org.jumpmind.symmetric.transport.BandwidthTestResults;
import org.jumpmind.symmetric.web.BandwidthSamplerUriHandler;
import org.junit.Assert;
import org.junit.Test;

public class BandwidthServiceUnitTest {

    @Test
    public void testDownloadKbps() throws Exception {

        BandwidthService service = new BandwidthService();
        service.log = new NoOpLog();
        int port = 9768;
        BandwidthServlet servlet = new BandwidthServlet();
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

    protected Server startServer(int port, String home, BandwidthServlet servlet)
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
    
    class BandwidthServlet extends BandwidthSamplerUriHandler implements Servlet {

        public void init(ServletConfig config) throws ServletException {
            
        }

        public ServletConfig getServletConfig() {
            return null;
        }

        public void service(ServletRequest req, ServletResponse res) throws ServletException,
                IOException {
           handle((HttpServletRequest)req, (HttpServletResponse)res);            
        }

        public String getServletInfo() {
            return null;
        }

        public void destroy() {
            
        }
        
    }
    
}