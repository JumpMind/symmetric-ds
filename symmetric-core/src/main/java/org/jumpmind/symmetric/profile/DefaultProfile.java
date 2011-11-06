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
package org.jumpmind.symmetric.profile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.SymmetricPropertiesFiles;
import org.jumpmind.symmetric.db.SqlScript;
import org.springframework.core.io.Resource;

/**
 * A default implementation of an {@link IProfile}.  This class may be subclassed 
 * or instantiated for use.
 */
public class DefaultProfile implements IProfile {

    private static final long serialVersionUID = 1L;

    final Log logger = LogFactory.getLog(getClass());

    protected String externalId;
    protected String nodeGroupId;
    protected String registrationUrl = "";
    protected URL configSqlFile;
    protected String name;
    protected String description;
    protected boolean needsConfig = true;

    public DefaultProfile() {
    }

    public DefaultProfile(String name, String description, boolean needsConfig) {
        this(null, null, null, name, description, needsConfig);
    }

    public DefaultProfile(String externalId, String nodeGroupId, URL configSqlFile,
            String name, String description, boolean needsConfig) {
        this.externalId = externalId;
        this.nodeGroupId = nodeGroupId;
        this.configSqlFile = configSqlFile;
        this.name = name;
        this.description = description;
        this.needsConfig = needsConfig;
    }
    
    public boolean isAutoRegister() {
        return false;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

    public URL getConfigSqlFile() {
        return configSqlFile;
    }

    public void setConfigSqlFile(URL configSqlFile) {
        this.configSqlFile = configSqlFile;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setRegistrationUrl(String registrationUrl) {
        this.registrationUrl = registrationUrl;
    }

    public String getRegistrationUrl() {
        return registrationUrl;
    }

    public void configure(ISymmetricEngine engine) throws Exception {
        if (needsConfig) {
            SymmetricPropertiesFiles propertiesFiles = engine.getApplicationContext().getBean(
                    SymmetricPropertiesFiles.class);
            Resource resource = propertiesFiles.findPreferredWritableResource();
            File file = resource.getFile();
            Properties properties = new Properties();
            if (file.exists()) {
                FileInputStream fis = new FileInputStream(file);
                properties.load(fis);
                fis.close();
            }

            boolean dirty = false;
            if (nodeGroupId != null) {
                properties.setProperty(ParameterConstants.NODE_GROUP_ID, nodeGroupId);
                dirty = true;
            }
            if (externalId != null) {
                properties.setProperty(ParameterConstants.EXTERNAL_ID, externalId);
                dirty = true;
            }

            if (registrationUrl != null) {
                properties.setProperty(ParameterConstants.REGISTRATION_URL, registrationUrl);
                dirty = true;
            }

            if (dirty) {
                if (!file.exists()) {
                    file.getParentFile().mkdirs();
                }
                logger.info("Updating " + file.getCanonicalPath());
                FileOutputStream fos = new FileOutputStream(file);
                properties.store(fos, "Saved by console at " + new Date());
                fos.close();
            }

            engine.getParameterService().rereadParameters();
            engine.getNodeService().deleteIdentity();
            if (configSqlFile != null) {
                logger.info("Applying " + configSqlFile.getFile());
                SqlScript script = new SqlScript(configSqlFile, engine.getDataSource());
                script.setLineDeliminator("\\\\n");
                script.execute();
            }
        }
    }

    public boolean isCompatible(ISymmetricEngine engine) {
        return true;
    }

    @Override
    public String toString() {
        return name;
    }


}
