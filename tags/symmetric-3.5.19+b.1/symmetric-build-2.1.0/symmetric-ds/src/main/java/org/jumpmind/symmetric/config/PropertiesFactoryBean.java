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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

public class PropertiesFactoryBean extends
        org.springframework.beans.factory.config.PropertiesFactoryBean {

    protected DynamicPropertiesFiles dynamicPropertiesFiles;
    
    private static Properties localProperties;

    public PropertiesFactoryBean() {
        this.setLocalOverride(true);
        if (localProperties != null) {
            this.setProperties(localProperties);
        }
    }
    
    public static void setLocalProperties(Properties localProperties) {
        PropertiesFactoryBean.localProperties = localProperties;
    }
    
    public static void clearLocalProperties() {
        PropertiesFactoryBean.localProperties = null;
    }
    
    @Override
    public void setLocations(Resource[] locations) {
        List<Resource> resources = new ArrayList<Resource>();
        resources.addAll(Arrays.asList(locations));
        if (dynamicPropertiesFiles != null) {
            for (String resource : dynamicPropertiesFiles) {
                resources
                        .add(new DefaultResourceLoader().getResource(resource));
            }
        }

        super.setLocations(resources.toArray(new Resource[resources.size()]));
    }    
    

    public void setDynamicPropertiesFiles(
            DynamicPropertiesFiles dynamicPropertiesFiles) {
        this.dynamicPropertiesFiles = dynamicPropertiesFiles;
    }
}