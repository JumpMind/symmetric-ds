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
