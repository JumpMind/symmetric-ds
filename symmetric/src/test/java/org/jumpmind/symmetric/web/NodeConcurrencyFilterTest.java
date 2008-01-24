package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NodeConcurrencyFilterTest {

    @Test(groups = "continuous", timeOut=120)
    public void testFilter() throws Exception {
        NodeConcurrencyFilter filter = new NodeConcurrencyFilter();
        filter.maxNumberOfConcurrentWorkers = 2;
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
