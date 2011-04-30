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

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.junit.Test;

public class SymmetricPropertiesFilesUnitTest {

    @Test
    public void testOverrideProperties1() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, "test.properties");
        SymmetricPropertiesFiles dpFiles = new SymmetricPropertiesFiles();
        Assert.assertEquals(1, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_1);
    }
    
    @Test
    public void testOverrideProperties1and2() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, "test1.properties");
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2, "test2.properties");
        SymmetricPropertiesFiles dpFiles = new SymmetricPropertiesFiles();
        Assert.assertEquals(2, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test1.properties"));
        Assert.assertTrue(dpFiles.contains("test2.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_2);
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_1);
    }
    
    @Test
    public void testOverrideProperties3andTemp() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_PREFIX + "3", "test3.properties");
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_TEMP, "test.temp.properties");
        SymmetricPropertiesFiles dpFiles = new SymmetricPropertiesFiles();
        Assert.assertEquals(2, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test3.properties"));
        Assert.assertTrue(dpFiles.contains("test.temp.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_PREFIX + "3");
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_TEMP);
    }

}