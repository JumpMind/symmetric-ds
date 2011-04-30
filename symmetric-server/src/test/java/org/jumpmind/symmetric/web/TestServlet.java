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

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

/**
 * Servlet implementation class for Servlet: TestServlet
 * 
 * 
 * <?xml version="1.0" encoding="UTF-8"?> <web-app id="WebApp_ID" version="2.4"
 * xmlns="http://java.sun.com/xml/ns/j2ee"
 * xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 * xsi:schemaLocation="http://java.sun.com/xml/ns/j2ee
 * http://java.sun.com/xml/ns/j2ee/web-app_2_4.xsd"> <display-name> testFilter</display-name>
 * <servlet> <description> </description> <display-name> TestServlet</display-name>
 * <servlet-name>TestServlet</servlet-name> <servlet-class>
 * org.jumpmind.symmetric.web.TestServlet</servlet-class> </servlet>
 * <servlet-mapping> <servlet-name>TestServlet</servlet-name>
 * <url-pattern>/TestServlet</url-pattern> </servlet-mapping>
 * <welcome-file-list> <welcome-file>index.html</welcome-file>
 * <welcome-file>index.htm</welcome-file> <welcome-file>index.jsp</welcome-file>
 * <welcome-file>default.html</welcome-file> <welcome-file>default.htm</welcome-file>
 * <welcome-file>default.jsp</welcome-file> </welcome-file-list> <filter>
 * <filter-name>ThrottleFilter</filter-name>
 * <filter-class>org.jumpmind.symmetric.web.ThrottleFilter</filter-class>
 * <init-param> <param-name>maxBps</param-name> <param-value>10240</param-value>
 * </init-param> <init-param> <param-name>threshold</param-name>
 * <param-value>8192</param-value> </init-param> <init-param>
 * <param-name>checkPoint</param-name> <param-value>4096</param-value>
 * </init-param> </filter> <filter-mapping> <filter-name>ThrottleFilter</filter-name>
 * <url-pattern>/TestServlet</url-pattern> </filter-mapping> </web-app>
 * 
 * 
 *
 * 
 */
public class TestServlet extends javax.servlet.http.HttpServlet implements javax.servlet.Servlet {
    static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(TestServlet.class);

    /*
     * (non-Java-doc)
     * 
     * @see javax.servlet.http.HttpServlet#HttpServlet()
     */
    public TestServlet() {
        super();
    }

    /*
     * (non-Java-doc)
     * 
     * @see javax.servlet.http.HttpServlet#doGet(HttpServletRequest request,
     *      HttpServletResponse response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        logger.info("in do get");

        final long rate = 10 * 1024;
        final long count = 50;
        final int bufferSize = 8192;

        System.out.println("Running for " + (bufferSize * count) + " bytes");

        PrintWriter out = response.getWriter();
        long start = System.currentTimeMillis();
        byte[] testBytes = new byte[bufferSize];

        Random r = new Random();

        r.nextBytes(testBytes);

        long i = 0;
        for (i = 0; i < count; i++) {
            out.print(new String(testBytes));

        }
        System.out.println();

        double expectedTime = (bufferSize * count) / rate;
        double actualTime = (System.currentTimeMillis() - start + 1) / 1000;
        System.out.println("Configured rate: " + rate);
        System.out.println("Actual rate: " + (i * bufferSize / actualTime));
        System.out.println("Expected time: " + expectedTime);
        System.out.println("Actual time: " + actualTime);
        assert (actualTime >= expectedTime - 2 && actualTime <= expectedTime + 2);

    }

    /*
     * (non-Java-doc)
     * 
     * @see javax.servlet.http.HttpServlet#doPost(HttpServletRequest request,
     *      HttpServletResponse response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        logger.info("in do post");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        super.doPut(request, response);
    }
}