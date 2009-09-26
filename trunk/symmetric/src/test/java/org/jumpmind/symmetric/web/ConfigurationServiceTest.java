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
package org.jumpmind.symmetric.web;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.impl.ConfigurationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationServiceTest extends AbstractDatabaseTest {

    public IConfigurationService configurationService;

    public ConfigurationServiceTest() throws Exception {
        super();
    }

    public ConfigurationServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        configurationService = (IConfigurationService) find(Constants.CONFIG_SERVICE);
    }

    @Test()
    public void testGetSuspendIgnoreChannels() throws Exception {
        // IParameterService parameterService = getParameterService();
        // parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS,
        // 3);

        String nodeId = "00000";

        Map<String, Set<String>> result = configurationService.getSuspendIgnoreChannelLists(nodeId);
        Assert.assertEquals(0, result.size());

        ConfigurationService configurationService = (ConfigurationService) find(Constants.CONFIG_SERVICE);

        List<NodeChannel> ncs = configurationService.getNodeChannels(nodeId);

        NodeChannel nc = ncs.get(1);
        String channelId = ncs.get(1).getId();

        nc.setSuspended(true);
        configurationService.saveNodeChannelControl(nc, false);

        result = configurationService.getSuspendIgnoreChannelLists(nodeId);
        ;

        Assert.assertEquals(1, result.size());
        // Assert.assertTrue(channelId.equals(result.get(WebConstants.SUSPENDED_CHANNELS)));
        Assert.assertTrue(result.get(WebConstants.SUSPENDED_CHANNELS).contains(nc.getId()));

        nc = ncs.get(0);
        nc.setSuspended(true);

        configurationService.saveNodeChannelControl(nc, false);

        // String channelIds = ncs.get(0).getId() + "," + ncs.get(1).getId();
        result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(WebConstants.SUSPENDED_CHANNELS).contains(ncs.get(0).getId()));
        Assert.assertTrue(result.get(WebConstants.SUSPENDED_CHANNELS).contains(ncs.get(1).getId()));
        // Assert.assertTrue(channelIds.equals(result.get(WebConstants.SUSPENDED_CHANNELS)));

        nc.setIgnored(true);
        configurationService.saveNodeChannelControl(nc, false);
        result = configurationService.getSuspendIgnoreChannelLists(nodeId);

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.get(WebConstants.SUSPENDED_CHANNELS).contains(ncs.get(0).getId()));
        Assert.assertTrue(result.get(WebConstants.SUSPENDED_CHANNELS).contains(ncs.get(1).getId()));
        // Assert.assertTrue(channelIds.equals(result.get(WebConstants.SUSPENDED_CHANNELS)));
        Assert.assertTrue(result.get(WebConstants.IGNORED_CHANNELS).contains(nc.getId()));
        // Assert.assertTrue(nc.getId().equals(result.get(WebConstants.IGNORED_CHANNELS)));

    }

}
