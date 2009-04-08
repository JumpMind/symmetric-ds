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

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(ParameterizedSuite.class)
@SuiteClasses( { SimpleIntegrationTest.class, LoadFromClientIntegrationTest.class, CleanupTest.class })
public class IntegrationTestSuite {

    static final String TEST_PREFIX = "test";

    @Parameters
    public static Collection<String[]> lookupClientServerDatabases() {
        return TestSetupUtil.lookupDatabasePairs(TEST_PREFIX);
    }

    String root;
    String client;

    public IntegrationTestSuite(String client, String root) {
        this.client = client;
        this.root = root;
    }

    @Test
    public void setup() throws Exception {
        TestSetupUtil.setup(TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT, client, root);
    }

}
