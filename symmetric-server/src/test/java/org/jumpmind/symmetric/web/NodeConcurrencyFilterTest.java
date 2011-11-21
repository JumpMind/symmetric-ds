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
 * under the License. 
 */

package org.jumpmind.symmetric.web;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class NodeConcurrencyFilterTest extends AbstractDatabaseTest {

    public NodeConcurrencyFilterTest() throws Exception {
        super();
    }

    @Test(timeout = 60000)
    public void testPullConcurrency() throws Exception {
        IParameterService parameterService = getParameterService();
        parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS, 3);

        NodeConcurrencyInterceptor filter = (NodeConcurrencyInterceptor) find(Constants.NODE_CONCURRENCY_INTERCEPTOR);

        MockWorker one = new MockWorker("00001", filter, "pull", "GET");
        MockWorker two = new MockWorker("00002", filter, "pull", "GET");
        MockWorker three = new MockWorker("00003", filter, "pull", "GET");
        MockWorker four = new MockWorker("00004", filter, "pull", "GET");

        one.start();
        two.start();
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

    @Test(timeout = 60000)
    public void testPushConcurrency() throws Exception {
        IParameterService parameterService = getParameterService();
        parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS, 2);

        NodeConcurrencyInterceptor filter = (NodeConcurrencyInterceptor) find(Constants.NODE_CONCURRENCY_INTERCEPTOR);

        IConcurrentConnectionManager manager = (IConcurrentConnectionManager) find(Constants.CONCURRENT_CONNECTION_MANGER);

        MockWorker one = new MockWorker("00001", filter, "push", "HEAD");
        MockWorker two = new MockWorker("00002", filter, "push", "HEAD");

        one.start();
        two.start();
        Thread.sleep(500);

        Assert.assertEquals(manager.getReservationCount("/sync/push"), 2);

        one = new MockWorker("00001", filter, "push", "PUT");
        two = new MockWorker("00002", filter, "push", "PUT");

        one.start();
        two.start();
        Thread.sleep(500);

        Assert.assertEquals(one.reached, true);
        Assert.assertEquals(two.reached, true);

        Assert.assertEquals(manager.getReservationCount("/sync/push"), 2);

        MockWorker five = new MockWorker("00005", filter, "push", "PUT");
        five.hold = false;
        five.start();
        Thread.sleep(500);

        Assert.assertEquals(five.reached, false);
        Assert.assertEquals(manager.getReservationCount("/sync/push"), 2);

        one.hold = false;
        two.hold = false;
        Thread.sleep(500);

        Assert.assertEquals(manager.getReservationCount("/sync/push"), 0);

    }

    class MockWorker extends Thread {

        private String servletPath;
        private String httpMethod;
        boolean inError;
        NodeConcurrencyInterceptor interceptor;
        String nodeId;
        boolean success = false;
        boolean hold = true;
        boolean reached = false;

        MockWorker(String nodeId, NodeConcurrencyInterceptor interceptor, String path,
                String httpMethod) {
            this.setDaemon(true);
            this.nodeId = nodeId;
            this.interceptor = interceptor;
            this.httpMethod = httpMethod;
            this.servletPath = path;
        }

        public void run() {

            MockHttpServletRequest req = new MockHttpServletRequest(httpMethod, "/sync/"
                    + servletPath);
            req.addParameter(WebConstants.NODE_ID, nodeId);
            req.setServletPath(servletPath);

            HttpServletResponse resp = new MockHttpServletResponse();
            try {
                if (interceptor.before(req, resp)) {
                    reached = true;
                    while (hold) {
                        AppUtils.sleep(50);
                    }
                    success = true;
                    interceptor.after(req, resp);
                }
            } catch (Exception e) {
                this.inError = true;
            }

        }

    }

    @Test
    public void testBuildSuspendIgnoreResponseHeaders() throws Exception {

        IConfigurationService configurationService = (IConfigurationService) find(Constants.CONFIG_SERVICE);

        String nodeId = "00000";

        NodeConcurrencyInterceptor filter = (NodeConcurrencyInterceptor) find(Constants.NODE_CONCURRENCY_INTERCEPTOR);
        MockHttpServletResponse resp = new MockHttpServletResponse();

        List<NodeChannel> ncs = configurationService.getNodeChannels(nodeId, false);

        ncs.get(0).setSuspendEnabled(false);
        ncs.get(0).setIgnoreEnabled(false);

        ncs.get(1).setSuspendEnabled(false);
        ncs.get(1).setIgnoreEnabled(false);

        ncs.get(2).setSuspendEnabled(false);
        ncs.get(2).setIgnoreEnabled(false);

        filter.buildSuspendIgnoreResponseHeaders(nodeId, resp);

        String suspended = (String) (resp.getHeader(WebConstants.SUSPENDED_CHANNELS));
        String ignored = (String) (resp.getHeader(WebConstants.IGNORED_CHANNELS));

        Assert.assertEquals("", suspended);
        Assert.assertEquals("", ignored);

        // Next, set some with "suspend" and some with "ignore"

        ncs = configurationService.getNodeChannels(nodeId, false);

        ncs.get(0).setSuspendEnabled(true);
        ncs.get(1).setIgnoreEnabled(true);
        ncs.get(2).setSuspendEnabled(true);

        configurationService.saveNodeChannelControl(ncs.get(0), false);
        configurationService.saveNodeChannelControl(ncs.get(1), false);
        configurationService.saveNodeChannelControl(ncs.get(2), false);

        filter.buildSuspendIgnoreResponseHeaders(nodeId, resp);

        suspended = (String) (resp.getHeader(WebConstants.SUSPENDED_CHANNELS));
        ignored = (String) (resp.getHeader(WebConstants.IGNORED_CHANNELS));

        Assert.assertEquals(suspended, ncs.get(0).getChannelId() + "," + ncs.get(2).getChannelId());
        Assert.assertEquals(ignored, ncs.get(1).getChannelId());

    }

}