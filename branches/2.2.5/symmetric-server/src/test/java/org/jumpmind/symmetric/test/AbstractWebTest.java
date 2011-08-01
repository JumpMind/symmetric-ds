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

import javax.servlet.ServletContext;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;

/**
 * 
 */
public class AbstractWebTest {

    protected ConfigurableWebApplicationContext applicationContext;
    protected ServletContext servletContext;

    @BeforeClass
    public static void setupTest() throws Exception {
        TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT, null,
                "h2");
        TestSetupUtil.getRootEngine().setup();  
    }

    @Before
    public void springTestContextBeforeTestMethod() throws Exception {
        servletContext = new MockServletContext();
        applicationContext = new XmlWebApplicationContext();
        applicationContext.setParent(TestSetupUtil.getRootEngine().getApplicationContext());
        applicationContext.setServletContext(servletContext);
        applicationContext.setConfigLocations(new String[0]);
        applicationContext.refresh();
        servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, applicationContext);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        TestSetupUtil.cleanup();
    }
}