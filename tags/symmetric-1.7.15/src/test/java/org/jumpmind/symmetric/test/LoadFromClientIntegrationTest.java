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
package org.jumpmind.symmetric.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class LoadFromClientIntegrationTest extends AbstractIntegrationTest {

    static final Log logger = LogFactory.getLog(LoadFromClientIntegrationTest.class);

    public LoadFromClientIntegrationTest() throws Exception {
    }

    public LoadFromClientIntegrationTest(String client, String root) throws Exception {
        super(client, root);
    }

    @Test(timeout = 30000)
    public void registerClientWithRoot() {
        getRootEngine().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        getClientEngine().start();
        String result = getClientEngine().reloadNode("00000");
        Assert.assertTrue(result, result.startsWith("Successfully opened initial load for node"));
        getClientEngine().push();
        JdbcTemplate jdbcTemplate = getClientDbDialect().getJdbcTemplate();
        int initialLoadEnabled = jdbcTemplate.queryForInt("select initial_load_enabled from sym_node_security where node_id='00000'");
        Assert.assertEquals(0, initialLoadEnabled);
    }

}
