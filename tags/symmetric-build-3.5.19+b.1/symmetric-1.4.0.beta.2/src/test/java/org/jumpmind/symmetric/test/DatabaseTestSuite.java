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

import org.jumpmind.symmetric.config.ParameterFilterTest;
import org.jumpmind.symmetric.db.DbTriggerTest;
import org.jumpmind.symmetric.extract.DataExtractorTest;
import org.jumpmind.symmetric.load.DataLoaderTest;
import org.jumpmind.symmetric.service.impl.AcknowledgeServiceTest;
import org.jumpmind.symmetric.service.impl.ClusterServiceTest;
import org.jumpmind.symmetric.service.impl.DataExtractorServiceTest;
import org.jumpmind.symmetric.service.impl.DataLoaderServiceTest;
import org.jumpmind.symmetric.service.impl.NodeServiceTest;
import org.jumpmind.symmetric.service.impl.OutgoingBatchServiceTest;
import org.jumpmind.symmetric.service.impl.ParameterServiceTest;
import org.jumpmind.symmetric.service.impl.PurgeServiceTest;
import org.jumpmind.symmetric.service.impl.RegistrationServiceTest;
import org.jumpmind.symmetric.service.impl.StatisticServiceTest;
import org.jumpmind.symmetric.web.NodeConcurrencyFilterTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(ParameterizedSuite.class)
@SuiteClasses( { DbTriggerTest.class, DataLoaderTest.class, DataExtractorTest.class, ParameterFilterTest.class,
        CrossCatalogSyncTest.class, FunkyDataTypesTest.class, NodeConcurrencyFilterTest.class,
        AcknowledgeServiceTest.class, ClusterServiceTest.class, DataExtractorServiceTest.class,
        DataLoaderServiceTest.class, NodeServiceTest.class, OutgoingBatchServiceTest.class, ParameterServiceTest.class,
        PurgeServiceTest.class, RegistrationServiceTest.class, StatisticServiceTest.class, CleanupTest.class })
public class DatabaseTestSuite extends AbstractDatabaseTest {

    public static final String DEFAULT_TEST_PREFIX = "test";

    @Parameters
    public static Collection<String[]> lookupDatabases() {
        return TestSetupUtil.lookupDatabases(DEFAULT_TEST_PREFIX);
    }

    public DatabaseTestSuite(String dbName) {
        super(dbName);
    }

    @Test
    public void setup() throws Exception {
        TestSetupUtil.setup(DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT, null, database);
    }

}
