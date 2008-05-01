package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NodeConcurrencyFilterTest extends AbstractDatabaseTest {

    @Test(groups = "continuous", timeOut=60000)
    public void testFilter() throws Exception {
        NodeConcurrencyFilter filter = (NodeConcurrencyFilter)getBeanFactory().getBean(Constants.NODE_CONCURRENCY_FILTER);
        filter.waitTimeBetweenRetriesInMs = 0;
        
        MockWorker one = new MockWorker(filter,"push");
        MockWorker two = new MockWorker(filter,"push");
        MockWorker three = new MockWorker(filter,"push");
        MockWorker other = new MockWorker(filter,"pull");
        
        one.start();
        Thread.sleep(500);
        
        two.start();
        Thread.sleep(500);
        
        three.start();        
        Thread.sleep(500);
        
        other.start();        
        Thread.sleep(500);
        
        one.hold = false;
        three.hold = false;
        other.hold = false;
        
        Thread.sleep(500);
        
        Assert.assertEquals(one.success, true);
        Assert.assertEquals(three.success, false);
        Assert.assertEquals(other.success, true);
        
        MockWorker four = new MockWorker(filter,"push");
        four.start();        
        Thread.sleep(500);
        
        two.hold = false;
        four.hold = false;
        
        Thread.sleep(500);
        
        Assert.assertEquals(two.success, true);
        Assert.assertEquals(four.success, true);

    }

    class MockWorker extends Thread {

        String servletPath;
        boolean hold = true;

        NodeConcurrencyFilter filter;

        boolean success;

        MockWorker(NodeConcurrencyFilter filter, String path) {
            this.setDaemon(true);
            this.filter = filter;
            this.servletPath = path;
        }

        public void run() {
            try {
                success = filter.doWork(servletPath, new NodeConcurrencyFilter.IWorker() {
                    public void work() throws ServletException, IOException {
                        while (hold) {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException();
            }
        }
    }
}
