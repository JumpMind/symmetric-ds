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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.AbstractCommandLauncher;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricAdmin;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroup;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.CustomizableThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public class SymmetricEngineHolder {

    final Logger log = LoggerFactory.getLogger(getClass());

    private static Map<String, ServerSymmetricEngine> staticEngines = new HashMap<String, ServerSymmetricEngine>();

    private static Set<EngineStarter> staticEnginesStarting = new HashSet<SymmetricEngineHolder.EngineStarter>();

    private Map<String, ServerSymmetricEngine> engines = new HashMap<String, ServerSymmetricEngine>();

    private Set<EngineStarter> enginesStarting = new TreeSet<SymmetricEngineHolder.EngineStarter>();

    private Map<String, List<String>> enginesFailed = new HashMap<String, List<String>>();
    
    private boolean staticEnginesMode = false;

    private boolean multiServerMode = false;

    private boolean autoStart = true;

    private boolean autoCreate = true;

    private ApplicationContext springContext;

    private String singleServerPropertiesFile;

    private static Date createTime = new Date();

    private int engineCount;

    private String deploymentType = "server";

    private boolean holderHasBeenStarted = false;

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

    public void setAutoCreate(boolean autoCreate) {
        this.autoCreate = autoCreate;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public void setStaticEnginesMode(boolean staticEnginesMode) {
        this.staticEnginesMode = staticEnginesMode;
    }

    public boolean isStaticEnginesMode() {
        return staticEnginesMode;
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
    
    public Map<String, List<String>> getFailedEngines() {
        return enginesFailed;
    }

    public boolean areEnginesInError() {
        return enginesFailed.size() > 0;
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
        enginesFailed.clear();
    }

    public void start() {
        try {
            SymmetricUtils.logNotices();

            if (staticEnginesMode) {
                log.info("In static engine mode");
                engines = staticEngines;
                enginesStarting = staticEnginesStarting;
            }

            if (autoCreate) {
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
                    
                    validateEngineFiles(files); 

                    if (files != null) {
                        for (int i = 0; i < files.length; i++) {
                            File file = files[i];
                            if (file.getName().endsWith(".properties")) {
                                engineCount++;
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
                
                ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(System.getProperty(SystemConstants.SYSPROP_CONCURRENT_ENGINES_STARTING_COUNT, "5")), new CustomizableThreadFactory("symmetric-engine-startup"));

                for (EngineStarter starter : enginesStarting) {
                    executor.execute(starter);
                }
                
                executor.shutdown();

            }

        } finally {
            holderHasBeenStarted = true;
        }

    }

    public void uninstallEngine(ISymmetricEngine engine) {
        Node node = engine.getNodeService().getCachedIdentity();
        String engineName = engine.getEngineName();
        File file = new SymmetricAdmin("uninstall", "", "")
                .findPropertiesFileForEngineWithName(engineName);
        engine.uninstall();
        engine.destroy();
        if (file != null) {
            file.delete();
        }
        getEngines().remove(engineName);

        for (ISymmetricEngine existingEngine : this.getEngines().values()) {
            existingEngine.removeAndCleanupNode(node.getNodeId());
        }        
    }

    public void setSpringContext(ApplicationContext applicationContext) {
        this.springContext = applicationContext;
    }

    public int getEngineCount() {
        return engineCount;
    }

    protected ISymmetricEngine create(String propertiesFile) {
        ServerSymmetricEngine engine = null;
        File file = new File(propertiesFile);
        String engineName = FilenameUtils.removeExtension(file.getName());
        try {

            Properties engineProperties = getEngineProperties(file);
            TypedProperties properties = new TypedProperties(engineProperties);
            engineName = getEngineName(properties);
            validateRequiredProperties(properties);
            
            engine = new ServerSymmetricEngine(propertiesFile != null ? file
                    : null, springContext, this);
            engine.setDeploymentType(deploymentType);
            synchronized (this) {
                if (!engines.containsKey(engine.getEngineName())) {
                    engines.put(engine.getEngineName(), engine);
                } else {
                    log.error(
                            "An engine with the name of {} was not started because an engine of the same name has already been started.  Please set the engine.name property in the properties file to a unique name.",
                            engine.getEngineName());
                    List<String> values = new ArrayList<String>();
                    values.add(engine.getEngineName());
                    values.add("An engine with the name of " + engine.getEngineName() + " was not started because an engine of the same name "
                            + "has already been started. Please set the engine.name property in the properties file to a unique name.");
                    enginesFailed.put(file.getName(), values);
                }

            }
            return engine;
        } catch (Exception e) {
            log.error("", e);
            List<String> values = new ArrayList<String>();
            values.add((engine == null ? engineName : engine.getEngineName()));
            values.add(e.getMessage());
            enginesFailed.put(file.getName(), values);
            return null;
        }
    }
    
    protected Properties getEngineProperties(File propertiesFile) throws Exception {
        Properties properties = new Properties();
        InputStream fileInputStream = new FileInputStream(propertiesFile.getAbsolutePath());
        properties.load(fileInputStream);
        return properties;
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
        
        String loadOnlyPassword = properties.getProperty(ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX + BasicDataSourcePropertyConstants.DB_POOL_PASSWORD);
        
        if (StringUtils.isNotBlank(loadOnlyPassword) && !loadOnlyPassword.startsWith(SecurityConstants.PREFIX_ENC)) {
            try {
                ISecurityService service = SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties);
                properties.setProperty(ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX + BasicDataSourcePropertyConstants.DB_POOL_PASSWORD,
                        SecurityConstants.PREFIX_ENC + service.encrypt(loadOnlyPassword));
            } catch (Exception ex) {
                log.warn("Could not encrypt load only password", ex);
            }
        }


        String engineName = validateRequiredProperties(properties);
        passedInProperties.setProperty(ParameterConstants.ENGINE_NAME, engineName);
        if (engines.get(engineName) != null) {
            try {
                engines.get(engineName).stop();
            } catch (Exception e) {
                log.error("", e);
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
                Collection<ServerSymmetricEngine> all = getEngines().values();
                for (ISymmetricEngine currentEngine : all) {
                    if (currentEngine.getParameterService().getSyncUrl()
                            .equals(registrationUrl)) {
                        String serverNodeGroupId = currentEngine.getParameterService()
                                .getNodeGroupId();
                        String clientNodeGroupId = properties
                                .getProperty(ParameterConstants.NODE_GROUP_ID);
                        String externalId = properties.getProperty(ParameterConstants.EXTERNAL_ID);

                        IConfigurationService configurationService = currentEngine
                                .getConfigurationService();
                        ITriggerRouterService triggerRouterService = currentEngine.
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
                                .getNodeGroupLinksFor(serverNodeGroupId, false);
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

                        IRegistrationService registrationService = currentEngine
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
            if (engine != null) {
                engineCount++;
                engine.start();
            } else {
                FileUtils.deleteQuietly(symmetricProperties);
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
        return !holderHasBeenStarted || enginesStarting.size() > 0;
    }

    public boolean hasAnyEngineInitialized() {
        if (enginesStarting.size() < engines.size()) {
            return true;
        }
        for (EngineStarter starter : enginesStarting) {
            ISymmetricEngine engine = starter.getEngine();
            if (engine != null && engine.isInitialized()) {
                return true;
            }
        }
        return false;
    }
    
    protected void validateEngineFiles(File[] files) {
        
        Map<String, String> dbToPropertyFiles = new LinkedHashMap<String, String>();
         
        for (File file : files) {
            if (file.getName().endsWith(".properties")) {
                // external.id
                Properties properties = new Properties();
                InputStream fileInputStream = null;
                try {
                    fileInputStream = new FileInputStream(file.getAbsolutePath());
                    properties.load(fileInputStream);                    
                    final String userUrl = String.format("%s@%s", 
                            properties.getProperty(BasicDataSourcePropertyConstants.DB_POOL_USER, ""), 
                            properties.getProperty(BasicDataSourcePropertyConstants.DB_POOL_URL, ""));                    
                    final String KEY = String.format("%s@%s", 
                            BasicDataSourcePropertyConstants.DB_POOL_USER, 
                            BasicDataSourcePropertyConstants.DB_POOL_URL);
                            
                    checkDuplicate(userUrl, KEY, dbToPropertyFiles, file);
                } catch (Exception ex) {
                    if (ex instanceof SymmetricException) {
                        log.error("**** FATAL **** error " + ex.toString()); // Jetty logs the stack at WARN level.
                        throw (SymmetricException)ex;
                    } else {                        
                        log.warn("Failed to validate engine properties file " + file, ex);
                    }
                } finally {
                    if (fileInputStream != null) {
                        IOUtils.closeQuietly(fileInputStream);
                    }
                }
            }
        }
    }
    
    protected void checkDuplicate(String value, String key, Map<String, String> values, File propertiesFile) {
        if (values.containsKey(value)) {
            throw new SymmetricException(String.format("Invalid configuration detected. 2 properties files reference "
                    + "the same %s: '%s'. Maybe an engines file was copied and needs to be moved. See: %s and %s.", 
                    key, value, values.get(value), propertiesFile.getAbsolutePath()));
        } else {
            values.put(value, propertiesFile.getAbsolutePath());
        }
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
            ParameterMetaData parameterMeta = ParameterConstants.getParameterMetaData().get(ParameterConstants.SYNC_URL);
            String defaultValue = "http://$(hostName):31415/sync/$(engineName)";
            if (parameterMeta != null) {
                defaultValue = parameterMeta.getDefaultValue();
            }

            log.debug("Defaulting node {} sync.url to {}", externalId, defaultValue);            
            properties.setProperty(ParameterConstants.SYNC_URL, defaultValue);
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

    static int threadNumber = 0;

    class EngineStarter implements Runnable, Comparable<EngineStarter> {

        String propertiesFile;
        ISymmetricEngine engine;

        public EngineStarter(String propertiesFile) {
            this.propertiesFile = propertiesFile;
        }

        @Override
        public void run() {
            engine = create(propertiesFile); 
            if (engine != null && autoStart &&
                    engine.getParameterService().is(ParameterConstants.AUTO_START_ENGINE)) {
                boolean started = engine.start();
                if (!started) {
                    File file = new File(propertiesFile);
                    List<String> values = new ArrayList<String>();
                    values.add(engine.getEngineName());
                    values.add(engine.getLastException());
                    enginesFailed.put(file.getName(), values);
                }
            }
            enginesStarting.remove(this);
        }
        
        public ISymmetricEngine getEngine() {
            return engine;
        }
        
        @Override
        public int compareTo(EngineStarter o) {
            return propertiesFile.compareTo(o.propertiesFile);
        }
    }
}