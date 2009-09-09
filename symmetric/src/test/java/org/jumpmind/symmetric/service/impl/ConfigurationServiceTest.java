/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.impl;

import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationServiceTest extends AbstractDatabaseTest {

    protected IConfigurationService configurationService;

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

    @Test
    public void testGetNodeChannels() throws Exception {
        List<NodeChannel> nodeChannels = configurationService.getNodeChannels();
        Assert.assertNotNull(nodeChannels);
        Assert.assertEquals(4, nodeChannels.size());
        for (NodeChannel nc : nodeChannels) {
            Assert.assertTrue("00000".equals(nc.getNodeId()));
            Assert.assertFalse(nc.isIgnored());
            Assert.assertFalse(nc.isSuspended());
            Assert.assertNull(nc.getLastExtractedTime());
        }

    }

    @Test
    public void testGetNodeChannelsById() throws Exception {
        String nodeId = "12345";
        List<NodeChannel> nodeChannels = configurationService.getNodeChannels(nodeId);
        Assert.assertNotNull(nodeChannels);

        Assert.assertEquals(4, nodeChannels.size());
        for (NodeChannel nc : nodeChannels) {
            Assert.assertTrue(nodeId.equals(nc.getNodeId()));
            Assert.assertFalse(nc.isIgnored());
            Assert.assertFalse(nc.isSuspended());
            Assert.assertNull(nc.getLastExtractedTime());
        }

        NodeChannel nc = nodeChannels.get(0);
        String updatedChannelId = nc.getId();

        // Test "ignored"
        nc.setIgnored(true);
        configurationService.saveNodeChannelControl(nc, false);
        NodeChannel compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId);
        Assert.assertTrue(compareTo.isIgnored());
        Assert.assertFalse(compareTo.isSuspended());
        Assert.assertNull(compareTo.getLastExtractedTime());

        // Test "suspended"
        compareTo.setSuspended(true);
        configurationService.saveNodeChannelControl(compareTo, false);
        compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId);
        Assert.assertTrue(compareTo.isIgnored());
        Assert.assertTrue(compareTo.isSuspended());
        Assert.assertNull(compareTo.getLastExtractedTime());

        // Test saving "last extracted time"
        NodeChannel nc1 = nodeChannels.get(1);
        String updatedChannelId1 = nc1.getId();

        Date date = new Date();
        nc1.setLastExtractedTime(date);
        configurationService.saveNodeChannelControl(nc1, false);

        compareTo = configurationService.getNodeChannel(updatedChannelId1, nodeId);
        Assert.assertFalse(compareTo.isIgnored());
        Assert.assertFalse(compareTo.isSuspended());
        Assert.assertNotNull(compareTo.getLastExtractedTime());
        Assert.assertEquals(date.getTime(), compareTo.getLastExtractedTime().getTime());

        // make sure other nodeChannel not effected
        compareTo = configurationService.getNodeChannel(updatedChannelId, nodeId);
        Assert.assertTrue(compareTo.isIgnored());
        Assert.assertTrue(compareTo.isSuspended());
        Assert.assertNull(compareTo.getLastExtractedTime());
    }

    @After
    public void reset() {

    }
}