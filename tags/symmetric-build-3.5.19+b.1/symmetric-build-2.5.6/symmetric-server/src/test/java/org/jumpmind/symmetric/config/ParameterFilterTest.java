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
package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Test;

public class ParameterFilterTest extends AbstractDatabaseTest {

    public ParameterFilterTest() throws Exception {
    }

    @Test
    public void testParameterFilter() {
        IParameterService service = find(Constants.PARAMETER_SERVICE);
        service.setParameterFilter(new IParameterFilter() {
            public String filterParameter(String key, String value) {
                if (key.equals("param.filter.test")) {
                    return "gotcha";
                } else {
                    return value;
                }
            }

            public boolean isAutoRegister() {
                return false;
            }
        });

        Assert.assertEquals(service.getString("param.filter.test"), "gotcha");
        Assert.assertEquals(service.getExternalId(), "00000");
    }
}