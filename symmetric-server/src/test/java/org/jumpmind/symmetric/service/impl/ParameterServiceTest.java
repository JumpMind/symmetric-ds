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

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
import org.junit.Test;

public class ParameterServiceTest extends AbstractDatabaseTest {

    public ParameterServiceTest() throws Exception {
        super();
    }

    @Test
    public void testParameterGetFromDefaults() {
        Assert.assertEquals("Unexpected default table prefix found.", getParameterService().getString(
                ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX), "sym");
    }

    @Test
    public void testParameterGetFromDatabase() {

        Assert.assertEquals(getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS), 2);

        getParameterService().saveParameter(ParameterConstants.CONCURRENT_WORKERS, 10);

        Assert.assertEquals(10, getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS));

        getParameterService().saveParameter(TestConstants.TEST_CLIENT_EXTERNAL_ID,
                TestConstants.TEST_CLIENT_NODE_GROUP, ParameterConstants.CONCURRENT_WORKERS, 15);

        // make sure we are not picking up someone else's parameter
        Assert.assertEquals(10, getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS));

        getParameterService().saveParameter(getParameterService().getExternalId(),
                getParameterService().getNodeGroupId(), ParameterConstants.CONCURRENT_WORKERS, 5);

        Assert.assertEquals(5, getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS));

    }

    @Test
    public void testBooleanParameter() {
        Assert.assertEquals(getParameterService().is("boolean.test"), false);
        getParameterService().saveParameter("boolean.test", true);
        Assert.assertEquals(getParameterService().is("boolean.test"), true);
    }

}