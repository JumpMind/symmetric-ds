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
package org.jumpmind.symmetric.web;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymmetricEngineHolder {

    final Logger log = LoggerFactory.getLogger(getClass());

    private Map<String, ServerSymmetricEngine> engines = new HashMap<String, ServerSymmetricEngine>();

    private Set<EngineStarter> enginesStarting = new HashSet<SymmetricEngineHolder.EngineStarter>();

    private boolean multiServerMode = false;

    private static Date createTime = new Date();

    private String deploymentType = "server";

    public Map<String, ServerSymmetricEngine> getEngines() {
        return engines;
    }

    public void setDeploymentType(String deploymentType) {
        this.deploymentType = deploymentType;
    }

    public void setMultiServerMode(boolean multiServerMode) {
        this.multiServerMode = multiServerMode;
    }

    public boolean isMultiServerMode() {
        return multiServerMode;
    }

    public boolean areEnginesConfigured() {
        return enginesStarting.size() > 0 || engines.size() > 0;
    }

    public String getEnginesDir() {
        String enginesDir = System.getProperty(Constants.SYS_PROP_ENGINES_DIR, "../engines");
        new File(enginesDir).mkdirs();
        return enginesDir;
    }

    public synchronized void stop() {
        Set<String> engineNames = engines.keySet();
        for (String engineName : engineNames) {
            engines.get(engineName).destroy();
        }
        engines.clear();
    }

    public void start() {
        if (isMultiServerMode()) {
            File enginesDir = new File(getEnginesDir());
            File[] files = enginesDir.listFiles();
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                if (file.getName().endsWith(".properties")) {
                    enginesStarting.add(new EngineStarter(file.getAbsolutePath()));
                }
            }

            for (EngineStarter starter : enginesStarting) {
                starter.start();
            }

        } else {
            ISymmetricEngine engine = create(null);
            engine.start();
        }
    }

    protected ISymmetricEngine create(String propertiesFile) {
        ServerSymmetricEngine engine = null;
        try {
            engine = new ServerSymmetricEngine(new File(propertiesFile));
            engine.setDeploymentType(deploymentType);
            if (!engines.containsKey(engine.getEngineName())) {
                engines.put(engine.getEngineName(), engine);
            } else {
                log.error(
                        "An engine with the name of {} was not started because an engine of the same name has already been started.  Please set the engine.name property in the properties file to a unique name.",
                        engine.getEngineName());
            }
            return engine;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public ISymmetricEngine install(Properties properties) throws Exception {
        String engineName = validateRequiredProperties(properties);
        if (engines.get(engineName) != null) {
            try {
                engines.get(engineName).stop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            engines.remove(engineName);
        }

        File enginesDir = new File(getEnginesDir());
        File symmetricProperties = new File(enginesDir, engineName + ".properties");
        FileOutputStream fileOs = null;
        try {
            fileOs = new FileOutputStream(symmetricProperties);
            properties.store(fileOs, "Updated by SymmetricDS Pro");
        } catch (IOException ex) {
            throw new RuntimeException("Failed to write symmetric.properties to engine directory",
                    ex);
        } finally {
            IOUtils.closeQuietly(fileOs);
        }

        String registrationUrl = properties.getProperty(ParameterConstants.REGISTRATION_URL);
        if (StringUtils.isNotBlank(registrationUrl)) {
            Collection<ServerSymmetricEngine> servers = getEngines().values();
            for (ISymmetricEngine symmetricWebServer : servers) {
                if (symmetricWebServer.getParameterService().getSyncUrl().equals(registrationUrl)) {
                    String nodeGroupId = properties.getProperty(ParameterConstants.NODE_GROUP_ID);
                    String externalId = properties.getProperty(ParameterConstants.EXTERNAL_ID);
                    IRegistrationService registrationService = symmetricWebServer
                            .getRegistrationService();
                    if (!registrationService.isAutoRegistration()
                            && !registrationService.isRegistrationOpen(nodeGroupId, externalId)) {
                        registrationService.openRegistration(nodeGroupId, externalId);
                    }
                }
            }
        }

        ISymmetricEngine engine = create(symmetricProperties.getAbsolutePath());
        engine.start();
        return engine;

    }

    public boolean areEnginesStarting() {
        return enginesStarting.size() > 0;
    }

    public String getEngineName(Properties properties) {
        String engineName = properties.getProperty(ParameterConstants.ENGINE_NAME);
        if (StringUtils.isBlank(engineName)) {
            String externalId = properties.getProperty(ParameterConstants.EXTERNAL_ID, "");
            String groupId = properties.getProperty(ParameterConstants.NODE_GROUP_ID, "");
            if (externalId.equals(groupId)) {
                engineName = groupId;
            } else {
                engineName = groupId + "-" + externalId;
            }
            engineName = properties.getProperty(ParameterConstants.ENGINE_NAME, engineName);
            String engineExt = "";
            int engineNumber = 0;
            while (new File(getEnginesDir(), engineName + engineExt + ".properties").exists()) {
                engineNumber++;
                engineExt = "-" + engineNumber;
            }
            engineName = engineName + engineExt;
        }
        return engineName;
    }

    public String validateRequiredProperties(Properties properties) {
        String externalId = properties.getProperty(ParameterConstants.EXTERNAL_ID);
        if (StringUtils.isBlank(externalId)) {
            throw new IllegalStateException("Missing property " + ParameterConstants.EXTERNAL_ID);
        }

        String groupId = properties.getProperty(ParameterConstants.NODE_GROUP_ID);
        if (StringUtils.isBlank(groupId)) {
            throw new IllegalStateException("Missing property " + ParameterConstants.NODE_GROUP_ID);
        }

        String engineName = getEngineName(properties);
        properties.setProperty(ParameterConstants.ENGINE_NAME, engineName);

        if (StringUtils.isBlank(properties.getProperty(ParameterConstants.SYNC_URL))) {
            throw new IllegalStateException("Missing property " + ParameterConstants.SYNC_URL);
        }
        if (StringUtils.isBlank(properties.getProperty(ParameterConstants.DBPOOL_DRIVER))) {
            throw new IllegalStateException("Missing property " + ParameterConstants.DBPOOL_DRIVER);
        }
        if (StringUtils.isBlank(properties.getProperty(ParameterConstants.DBPOOL_URL))) {
            throw new IllegalStateException("Missing property " + ParameterConstants.DBPOOL_URL);
        }
        if (!properties.containsKey(ParameterConstants.DBPOOL_USER)) {
            throw new IllegalStateException("Missing property " + ParameterConstants.DBPOOL_USER);
        }
        if (!properties.containsKey(ParameterConstants.DBPOOL_PASSWORD)) {
            throw new IllegalStateException("Missing property "
                    + ParameterConstants.DBPOOL_PASSWORD);
        }
        if (!properties.containsKey(ParameterConstants.REGISTRATION_URL)) {
            properties.setProperty(ParameterConstants.REGISTRATION_URL, "");
        }
        return engineName;
    }

    public static Date getCreateTime() {
        return createTime;
    }

    class EngineStarter extends Thread {

        String propertiesFile;

        public EngineStarter(String propertiesFile) {
            super("symmetric-startup");
            this.propertiesFile = propertiesFile;
        }

        @Override
        public void run() {
            if (propertiesFile != null) {
                ISymmetricEngine engine = create(propertiesFile);
                if (engine != null) {
                    engine.start();
                }
            }
            enginesStarting.remove(this);
        }
    }

}
