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

import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * ,
 */
public class ConfigurationServiceTest extends AbstractDatabaseTest {

    protected IConfigurationService configurationService;

    public ConfigurationServiceTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        configurationService = (IConfigurationService) find(Constants.CONFIG_SERVICE);
    }

    @Test
    public void testGetNodeChannels() throws Exception {
        List<NodeChannel> nodeChannels = configurationService.getNodeChannels(false);
        Assert.assertNotNull(nodeChannels);
        Assert.assertEquals(4, nodeChannels.size());
        for (NodeChannel nc : nodeChannels) {
            Assert.assertTrue("00000".equals(nc.getNodeId()));
            Assert.assertFalse(nc.isIgnoreEnabled());
            Assert.assertFalse(nc.isSuspendEnabled());
            Assert.assertNull(nc.getLastExtractedTime());
        }

    }

    @Test
    public void testGetNodeChannelsById() throws Exception {
        String nodeId = "12345";
        List<NodeChannel> nodeChannels = configurationService.getNodeChannels(nodeId, false);
        Assert.assertNotNull(nodeChannels);

        Assert.assertEquals(4, nodeChannels.size());
        for (NodeChannel nc : nodeChannels) {
            Assert.assertTrue(nodeId.equals(nc.getNodeId()));
            Assert.assertFalse(nc.isIgnoreEnabled());
            Assert.assertFalse(nc.isSuspendEnabled());
            Assert.assertNull(nc.getLastExtractedTime());
        }

        NodeChannel nc = nodeChannels.get(0);
        String updatedChannelId = nc.getChannelId();

        // Test "ignored"
        nc.setIgnoreEnabled(true);
        configurationService.saveNodeChannelControl(nc, false);
        NodeChannel compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId, false);
        Assert.assertTrue(compareTo.isIgnoreEnabled());
        Assert.assertFalse(compareTo.isSuspendEnabled());
        Assert.assertNull(compareTo.getLastExtractedTime());

        // Test "suspended"
        compareTo.setSuspendEnabled(true);
        configurationService.saveNodeChannelControl(compareTo, false);
        compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId, false);
        Assert.assertTrue(compareTo.isIgnoreEnabled());
        Assert.assertTrue(compareTo.isSuspendEnabled());
        Assert.assertNull(compareTo.getLastExtractedTime());

        // Test saving "last extracted time"
        NodeChannel nc1 = nodeChannels.get(1);
        String updatedChannelId1 = nc1.getChannelId();

        Date date = new Date();
        nc1.setLastExtractedTime(date);
        configurationService.saveNodeChannelControl(nc1, false);

        compareTo = configurationService.getNodeChannel(updatedChannelId1, nodeId, false);
        Assert.assertFalse(compareTo.isIgnoreEnabled());
        Assert.assertFalse(compareTo.isSuspendEnabled());
        Assert.assertNotNull(compareTo.getLastExtractedTime());
        Assert.assertEquals(date.getTime(), compareTo.getLastExtractedTime().getTime());

        // make sure other nodeChannel not effected
        compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId, false);
        Assert.assertTrue(compareTo.isIgnoreEnabled());
        Assert.assertTrue(compareTo.isSuspendEnabled());
        Assert.assertNull(compareTo.getLastExtractedTime());
    }

    @Test()
    public void testGetSuspendIgnoreChannels() throws Exception {
        String nodeId = "00000";

        ChannelMap result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertEquals(result.getSuspendChannels().size(), 0);
        Assert.assertEquals(result.getIgnoreChannels().size(), 0);

        ConfigurationService configurationService = (ConfigurationService) find(Constants.CONFIG_SERVICE);

        List<NodeChannel> ncs = configurationService.getNodeChannels(nodeId, false);

        NodeChannel nc = ncs.get(1);

        nc.setSuspendEnabled(true);
        configurationService.saveNodeChannelControl(nc, false);

        result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertTrue(result.getSuspendChannels().contains(nc.getChannelId()));

        nc = ncs.get(0);
        nc.setSuspendEnabled(true);

        configurationService.saveNodeChannelControl(nc, false);

        // String channelIds = ncs.get(0).getId() + "," + ncs.get(1).getId();
        result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertTrue(result.getSuspendChannels().contains(ncs.get(0).getChannelId()));
        Assert.assertTrue(result.getSuspendChannels().contains(ncs.get(1).getChannelId()));

        nc.setIgnoreEnabled(true);
        configurationService.saveNodeChannelControl(nc, false);
        result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertTrue(result.getSuspendChannels().contains(ncs.get(0).getChannelId()));
        Assert.assertTrue(result.getSuspendChannels().contains(ncs.get(1).getChannelId()));
        Assert.assertTrue(result.getIgnoreChannels().contains(nc.getChannelId()));
    }

    @After
    public void reset() {

    }
}