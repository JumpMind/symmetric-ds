package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NodeConcurrencyFilterTest extends AbstractDatabaseTest {

    @Test(groups = "continuous", timeOut = 60000)
    public void testPullConcurrency() throws Exception {
        IParameterService parameterService = getParameterService();
        parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS, 3);

        NodeConcurrencyFilter filter = (NodeConcurrencyFilter) getBeanFactory().getBean(
                Constants.NODE_CONCURRENCY_FILTER);

        MockWorker one = new MockWorker("00001", filter, "pull", "GET");
        MockWorker two = new MockWorker("00002", filter, "pull", "GET");
        MockWorker three = new MockWorker("00003", filter, "pull", "GET");
        MockWorker four = new MockWorker("00004", filter, "pull", "GET");

        one.start();
        Thread.sleep(500);

        two.start();
        Thread.sleep(500);

        three.start();
        Thread.sleep(500);

        four.start();
        Thread.sleep(500);

        Assert.assertEquals(one.reached, true);
        Assert.assertEquals(two.reached, true);
        Assert.assertEquals(three.reached, true);
        Assert.assertEquals(four.reached, false);

        one.hold = false;
        two.hold = false;
        three.hold = false;
        four.hold = false;

        Thread.sleep(500);

        Assert.assertEquals(one.success, true);
        Assert.assertEquals(two.success, true);
        Assert.assertEquals(three.success, true);
        Assert.assertEquals(four.success, false);

        MockWorker five = new MockWorker("00005", filter, "pull", "GET");
        five.hold = false;
        five.start();
        Thread.sleep(500);

        Assert.assertEquals(five.success, true);

    }
    
    @Test(groups = "continuous", timeOut = 60000)
    public void testPushConcurrency() throws Exception {
        IParameterService parameterService = getParameterService();
        parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS, 2);

        NodeConcurrencyFilter filter = (NodeConcurrencyFilter) getBeanFactory().getBean(
                Constants.NODE_CONCURRENCY_FILTER);
        
        IConcurrentConnectionManager manager = (IConcurrentConnectionManager)getBeanFactory().getBean(Constants.CONCURRENT_CONNECTION_MANGER);

        MockWorker one = new MockWorker("00001", filter, "push", "HEAD");
        MockWorker two = new MockWorker("00002", filter, "push", "HEAD");
        
        one.start();
        two.start();
        Thread.sleep(500);
        
        Assert.assertEquals(manager.getReservationCount("push"), 2);
               
        one = new MockWorker("00001", filter, "push", "PUT");
        two = new MockWorker("00002", filter, "push", "PUT");

        one.start();
        two.start();
        Thread.sleep(500);

        Assert.assertEquals(one.reached, true);
        Assert.assertEquals(two.reached, true);
        
        Assert.assertEquals(manager.getReservationCount("push"), 2);

        MockWorker five = new MockWorker("00005", filter, "push", "PUT");
        five.hold = false;
        five.start();
        Thread.sleep(500);

        Assert.assertEquals(five.reached, false);
        Assert.assertEquals(manager.getReservationCount("push"), 2);
        
        one.hold = false;
        two.hold = false;
        Thread.sleep(500);
        
        Assert.assertEquals(manager.getReservationCount("push"), 0);

    }
    

    class MockWorker extends Thread {

        private String servletPath;
        private String httpMethod;
        Exception inError;
        NodeConcurrencyFilter filter;
        String nodeId;
        boolean success = false;
        boolean hold = true;
        boolean reached = false;

        MockWorker(String nodeId, NodeConcurrencyFilter filter, String path, String httpMethod) {
            this.setDaemon(true);
            this.nodeId = nodeId;
            this.filter = filter;
            this.httpMethod = httpMethod;
            this.servletPath = path;
        }

        public void run() {

            MockHttpServletRequest req = new MockHttpServletRequest(httpMethod, "/sync/" + servletPath);
            req.addParameter(WebConstants.NODE_ID, nodeId);
            req.setServletPath(servletPath);

            HttpServletResponse resp = new MockHttpServletResponse();
            try {
                filter.doFilter(req, resp, new FilterChain() {
                    public void doFilter(ServletRequest request, ServletResponse response) throws IOException,
                            ServletException {
                        reached = true;

                        while (hold) {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                            }
                        }

                        success = true;
                    }
                });
            } catch (Exception e) {
                this.inError = e;
            }

        }

    }
}
