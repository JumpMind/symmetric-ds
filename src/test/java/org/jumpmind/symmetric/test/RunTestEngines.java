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

import org.jumpmind.symmetric.SymmetricEngine;

/**
 * Simple test utility class to help with stand-alone testing. Run this class
 * from your development environment to get a SymmetricDS client and server you
 * can play with.
 */
public class RunTestEngines {

    public static void main(String[] args) throws Exception {
        String[] databases = TestSetupUtil.lookupDatabasePairs(DatabaseTestSuite.DEFAULT_TEST_PREFIX).iterator().next();
        TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT,
                databases[0], databases[1]);
        SymmetricEngine root = TestSetupUtil.getRootEngine();
        SymmetricEngine client = TestSetupUtil.getClientEngine();
        root.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        client.start();
        while (true) {
            client.pull();
            client.push();
            Thread.sleep(10000);
        }
    }

}
