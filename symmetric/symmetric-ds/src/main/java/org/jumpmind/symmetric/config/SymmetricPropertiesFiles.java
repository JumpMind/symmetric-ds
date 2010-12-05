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

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * A list of properties files that SymmetricDS uses to determine it's configuration.
 * It has support for properties files that were set via system properties so that if the
 * system property changes down the road, the initially configured files remain
 * captured during a properties refresh.
 */
public class SymmetricPropertiesFiles extends ArrayList<String> {

    private static final long serialVersionUID = 1L;
    private static ILog log = LogFactory.getLog(SymmetricPropertiesFiles.class);

    public SymmetricPropertiesFiles() {
        this(new ArrayList<String>(0));
    }
       
    public SymmetricPropertiesFiles(List<String> resources) {
        
        addAll(resources);
        
        File file = new File(System.getProperty("user.dir"), "symmetric.properties");
        if (file.exists() && file.isFile()) {
            try {
                add(file.toURI().toURL().toExternalForm());
            } catch (MalformedURLException e) {
                log.error(e);
            }
        }
        
        Properties systemProperties = System.getProperties();
        for (Object key : systemProperties.keySet()) {
            if (key.toString().startsWith(Constants.OVERRIDE_PROPERTIES_FILE_PREFIX)) {
                add(System.getProperty(key.toString()));
            }
        }
    }
       
}