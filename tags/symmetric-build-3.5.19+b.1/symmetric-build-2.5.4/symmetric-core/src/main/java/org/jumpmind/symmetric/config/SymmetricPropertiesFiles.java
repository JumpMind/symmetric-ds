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
 * under the License. 
 */
package org.jumpmind.symmetric.config;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

/**
 * A list of properties files that SymmetricDS uses to determine it's
 * configuration. It has support for properties files that were set via system
 * properties so that if the system property changes down the road, the
 * initially configured files remain captured during a properties refresh.
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

    /**
     * Find a configured properties file that we are allowed to write to.
     */
    public Resource findWriteableOverrideResource(boolean exists) {
        DefaultResourceLoader resourceLoader = new DefaultResourceLoader();
        for (int i = size() - 1; i >= 0; i--) {
            String resourcePath = StringUtils.replace(get(i), "${user.home}",
                    System.getProperty("user.home"));
            if (resourcePath.startsWith("file:")) {
                Resource resource = resourceLoader.getResource(resourcePath);
                try {
                    File file = resource.getFile();
                    if (!exists || (exists && file.exists())) {
                        return resource;
                    }
                } catch (IOException ex) {
                }
            }
        }
        return null;
    }

    public Resource findPreferredWritableResource() {
        Resource resource = findWriteableOverrideResource(true);
        if (resource == null) {
            resource = findWriteableOverrideResource(false);
        }
        return resource;
    }

}