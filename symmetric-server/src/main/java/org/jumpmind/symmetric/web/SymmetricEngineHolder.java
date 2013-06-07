/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.AbstractCommandLauncher;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroup;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymmetricEngineHolder {

    final Logger log = LoggerFactory.getLogger(getClass());

    private Map<String, ServerSymmetricEngine> engines = new HashMap<String, ServerSymmetricEngine>();

    private Set<EngineStarter> enginesStarting = new HashSet<SymmetricEngineHolder.EngineStarter>();

    private boolean multiServerMode = false;

    private boolean autoStart = true;

    private String singleServerPropertiesFile;

    private static Date createTime = new Date();

    private int engineCount;

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

    public void setSingleServerPropertiesFile(String singleServerPropertiesFile) {
        this.singleServerPropertiesFile = singleServerPropertiesFile;
    }

    public String getSingleServerPropertiesFile() {
        return singleServerPropertiesFile;
    }

    public boolean areEnginesConfigured() {
        return enginesStarting.size() > 0 || engines.size() > 0;
    }

    public int getNumerOfEnginesStarting() {
        return enginesStarting.size();
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public boolean isAutoStart() {
        return autoStart;
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
            File enginesDir = new File(AbstractCommandLauncher.getEnginesDir());
            File[] files = null;

            if (enginesDir != null) {
                files = enginesDir.listFiles();
            }

            if (files == null) {
                String firstAttempt = enginesDir.getAbsolutePath();
                enginesDir = new File(".");
                log.warn(
                        "Unable to retrieve engine properties files from {}.  Trying current working directory {}",
                        firstAttempt, enginesDir.getAbsolutePath());

                if (enginesDir != null) {
                    files = enginesDir.listFiles();
                }
            }

            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    engineCount++;
                    File file = files[i];
                    if (file.getName().endsWith(".properties")) {
                        enginesStarting.add(new EngineStarter(file.getAbsolutePath()));
                    }
                }
            } else {
                log.error("Unable to retrieve engine properties files from default location or from current working directory.  No engines to start.");
            }

        } else {
            engineCount++;
            enginesStarting.add(new EngineStarter(singleServerPropertiesFile));
        }

        for (EngineStarter starter : enginesStarting) {
            starter.start();
        }

    }

    public int getEngineCount() {
        return engineCount;
    }

    protected ISymmetricEngine create(String propertiesFile) {
        ServerSymmetricEngine engine = null;
        try {
            engine = new ServerSymmetricEngine(propertiesFile != null ? new File(propertiesFile)
                    : null);
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

    public ISymmetricEngine install(Properties passedInProperties) throws Exception {
        TypedProperties properties = new TypedProperties(passedInProperties);
        String password = properties.getProperty(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD);
        if (StringUtils.isNotBlank(password) && !password.startsWith(SecurityConstants.PREFIX_ENC)) {
            try {
                ISecurityService service = SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties);
                properties.setProperty(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD,
                        SecurityConstants.PREFIX_ENC + service.encrypt(password));
            } catch (Exception ex) {
                log.warn("Could not encrypt password", ex);
            }
        }

        String engineName = validateRequiredProperties(properties);
        passedInProperties.setProperty(ParameterConstants.ENGINE_NAME, engineName);
        if (engines.get(engineName) != null) {
            try {
                engines.get(engineName).stop();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            engines.remove(engineName);
        }

        File enginesDir = new File(AbstractCommandLauncher.getEnginesDir());
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

        ISymmetricEngine engine = null;
        try {

            String registrationUrl = properties.getProperty(ParameterConstants.REGISTRATION_URL);
            if (StringUtils.isNotBlank(registrationUrl)) {
                Collection<ServerSymmetricEngine> servers = getEngines().values();
                for (ISymmetricEngine symmetricWebServer : servers) {
                    if (symmetricWebServer.getParameterService().getSyncUrl()
                            .equals(registrationUrl)) {
                        String serverNodeGroupId = symmetricWebServer.getParameterService()
                                .getNodeGroupId();
                        String clientNodeGroupId = properties
                                .getProperty(ParameterConstants.NODE_GROUP_ID);
                        String externalId = properties.getProperty(ParameterConstants.EXTERNAL_ID);

                        IConfigurationService configurationService = symmetricWebServer
                                .getConfigurationService();
                        ITriggerRouterService triggerRouterService = symmetricWebServer.
                                getTriggerRouterService();
                        List<NodeGroup> groups = configurationService.getNodeGroups();
                        boolean foundGroup = false;
                        for (NodeGroup nodeGroup : groups) {
                            if (nodeGroup.getNodeGroupId().equals(clientNodeGroupId)) {
                                foundGroup = true;
                            }
                        }

                        if (!foundGroup) {
                            configurationService.saveNodeGroup(new NodeGroup(clientNodeGroupId));
                        }

                        boolean foundLink = false;
                        List<NodeGroupLink> links = configurationService
                                .getNodeGroupLinksFor(serverNodeGroupId);
                        for (NodeGroupLink nodeGroupLink : links) {
                            if (nodeGroupLink.getTargetNodeGroupId().equals(clientNodeGroupId)) {
                                foundLink = true;
                            }
                        }

                        if (!foundLink) {
                            configurationService.saveNodeGroupLink(new NodeGroupLink(
                                    serverNodeGroupId, clientNodeGroupId, NodeGroupLinkAction.W));
                            triggerRouterService.syncTriggers();
                        }

                        IRegistrationService registrationService = symmetricWebServer
                                .getRegistrationService();
                        if (!registrationService.isAutoRegistration()
                                && !registrationService.isRegistrationOpen(clientNodeGroupId,
                                        externalId)) {
                            Node node = new Node(properties);
                            registrationService.openRegistration(node);
                        }
                    }
                }
            }

            engine = create(symmetricProperties.getAbsolutePath());
            if (engine != null && autoStart) {
                engine.start();
            } else {
                log.warn("The engine could not be created.  It will not be started");
            }
            return engine;

        } catch (RuntimeException ex) {
            if (engine != null) {
                engine.destroy();
            }
            FileUtils.deleteQuietly(symmetricProperties);
            throw ex;
        }

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
            while (new File(AbstractCommandLauncher.getEnginesDir(), engineName + engineExt
                    + ".properties").exists()) {
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
        if (StringUtils.isBlank(properties
                .getProperty(BasicDataSourcePropertyConstants.DB_POOL_DRIVER))) {
            throw new IllegalStateException("Missing property "
                    + BasicDataSourcePropertyConstants.DB_POOL_DRIVER);
        }
        if (StringUtils.isBlank(properties
                .getProperty(BasicDataSourcePropertyConstants.DB_POOL_URL))) {
            throw new IllegalStateException("Missing property "
                    + BasicDataSourcePropertyConstants.DB_POOL_URL);
        }
        if (!properties.containsKey(BasicDataSourcePropertyConstants.DB_POOL_USER)) {
            throw new IllegalStateException("Missing property "
                    + BasicDataSourcePropertyConstants.DB_POOL_USER);
        }
        if (!properties.containsKey(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD)) {
            throw new IllegalStateException("Missing property "
                    + BasicDataSourcePropertyConstants.DB_POOL_PASSWORD);
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
            ISymmetricEngine engine = create(propertiesFile);
            if (engine != null && autoStart) {
                engine.start();
            }
            enginesStarting.remove(this);
        }
    }
}