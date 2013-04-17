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

package org.jumpmind.symmetric.test;

import java.util.Collection;

import org.jumpmind.symmetric.config.ParameterFilterTest;
import org.jumpmind.symmetric.extract.DataExtractorTest;
import org.jumpmind.symmetric.integrate.XmlPublisherFilterTest;
import org.jumpmind.symmetric.load.AdditiveDataLoaderFilterTest;
import org.jumpmind.symmetric.load.DataLoaderTest;
import org.jumpmind.symmetric.map.DataLoaderMappingTest;
import org.jumpmind.symmetric.service.impl.AcknowledgeServiceTest;
import org.jumpmind.symmetric.service.impl.ClusterServiceTest;
import org.jumpmind.symmetric.service.impl.DataExtractorServiceTest;
import org.jumpmind.symmetric.service.impl.DataLoaderServiceTest;
import org.jumpmind.symmetric.service.impl.DataServiceTest;
import org.jumpmind.symmetric.service.impl.NodeServiceTest;
import org.jumpmind.symmetric.service.impl.OutgoingBatchServiceTest;
import org.jumpmind.symmetric.service.impl.ParameterServiceTest;
import org.jumpmind.symmetric.service.impl.PurgeServiceTest;
import org.jumpmind.symmetric.service.impl.RegistrationServiceTest;
import org.jumpmind.symmetric.service.impl.StatisticServiceTest;
import org.jumpmind.symmetric.service.impl.TriggerRouterServiceTest;
import org.jumpmind.symmetric.web.NodeConcurrencyFilterTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(ParameterizedSuite.class)
@SuiteClasses( { TriggerRouterServiceTest.class, DataLoaderTest.class, DataLoaderMappingTest.class, DataExtractorTest.class,
        ParameterFilterTest.class, CrossCatalogSyncTest.class, FunkyDataTypesTest.class,
        NodeConcurrencyFilterTest.class, AcknowledgeServiceTest.class, ClusterServiceTest.class,
        DataExtractorServiceTest.class, DataLoaderServiceTest.class, NodeServiceTest.class,
        OutgoingBatchServiceTest.class, ParameterServiceTest.class, PurgeServiceTest.class,
        RegistrationServiceTest.class, StatisticServiceTest.class, XmlPublisherFilterTest.class,
        AdditiveDataLoaderFilterTest.class, DataServiceTest.class, CleanupTest.class })
/**
 * 
 */
public class DatabaseTestSuite {

    String database;
    
    public static final String DEFAULT_TEST_PREFIX = "test";

    public DatabaseTestSuite() throws Exception {
    }
    
    public void init(String database) {
        this.database = database;
    }
    
    @Parameters
    public static Collection<String[]> lookupDatabases() {
        return TestSetupUtil.lookupDatabases(DEFAULT_TEST_PREFIX);
    }

    @Test
    public void setup() throws Exception {
        AbstractDatabaseTest.standalone = false;
        TestSetupUtil.setup(DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT, null, database);
    }

}