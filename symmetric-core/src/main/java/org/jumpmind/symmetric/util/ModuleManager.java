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
package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModuleManager {

    private final static Logger log = LoggerFactory.getLogger(ModuleManager.class);

    private static final String EXT_PROPERTIES = ".properties";

    private static final String EXT_JAR = "jar";

    private static final String PROP_REPOSITORIES = "repos";

    private static final String PROP_VERSION = "sym.version";
    
    private static final String PROP_DRIVER = "driver.";

    private static ModuleManager instance;
    
    private Properties properties = new Properties();

    private Map<String, List<MavenArtifact>> modules = new TreeMap<String, List<MavenArtifact>>();
    
    private Map<String, String> driverToModule = new HashMap<String, String>();

    private List<String> repos = new ArrayList<String>();
    
    private String modulesDir;

    private ModuleManager() throws ModuleException {
        String sysModulesDir = System.getProperty(SystemConstants.SYSPROP_MODULES_DIR);
        if (StringUtils.isNotBlank(sysModulesDir)) {
            modulesDir = sysModulesDir;
        } else if ("true".equals(System.getProperty(SystemConstants.SYSPROP_LAUNCHER))) {
            modulesDir = joinDirName(AppUtils.getSymHome(), "lib");
        } else {
            modulesDir = ".";
        }
        File dir = new File(modulesDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        try (InputStream in = getClass().getResourceAsStream("/symmetric-modules.properties")) {
            properties.load(in);

            for (Entry<Object, Object> entry : properties.entrySet()) {
                String key = entry.getKey().toString();
                if (key.equals(PROP_REPOSITORIES)) {
                    for (String repo : entry.getValue().toString().split(MavenArtifact.REGEX_LIST)) {
                        repos.add(repo);
                    }
                } else if (key.startsWith(PROP_DRIVER)) {
                    String[] drivers = entry.getValue().toString().split(MavenArtifact.REGEX_LIST);
                    for (String driver : drivers) {
                        driverToModule.put(driver, key.substring(PROP_DRIVER.length()));
                    }
                } else {
                    List<MavenArtifact> list = new ArrayList<MavenArtifact>();
                    for (String dependency : entry.getValue().toString().split(MavenArtifact.REGEX_LIST)) {
                        list.add(new MavenArtifact(dependency));
                    }
                    modules.put(key, list);
                }
            }
        } catch (Exception e) {
            logAndThrow("Unable to read symmetric-modules.properties file: " + e.getMessage(), e);
        }
    }

    public static ModuleManager getInstance() throws ModuleException {
        if (instance == null) {
            instance = new ModuleManager();
        }
        return instance;
    }

    public void install(String moduleId) throws ModuleException {
        checkModuleInstalled(moduleId, false);
        List<MavenArtifact> artifacts = resolveArtifacts(moduleId);
        log.info("Installing module {} with {} artifacts", moduleId, artifacts.size());

        for (MavenArtifact artifact : artifacts) {
            String fileName = buildFileName(modulesDir, artifact, EXT_JAR);
            if (new File(fileName).exists()) {
                log.info("{} already exists", fileName);
            } else {
                boolean installedOkay = false;
                String errorMessage = null;
                for (String repo : repos) {
                    String urlString = buildUrl(repo, artifact, EXT_JAR);

                    try {
                        URL url = new URL(urlString);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("GET");
                        conn.setInstanceFollowRedirects(true);
                        conn.setConnectTimeout(10000);
                        conn.setReadTimeout(30000);

                        if (conn.getResponseCode() == 200) {
                            log.info("Downloading {}", urlString);

                            try (InputStream in = conn.getInputStream();
                                    FileOutputStream out = new FileOutputStream(fileName)) {
                                IOUtils.copy(in, out);
                                installedOkay = true;
                                break;
                            } catch (Exception e) {
                                errorMessage = "Unable to download from " + urlString + " because: " + e.getMessage();
                                log.error(errorMessage);
                            }
                        } else {
                            errorMessage = "Unable to download " + urlString + " because [" + conn.getResponseCode() + "]";
                            log.debug(errorMessage);
                        }
                    } catch (MalformedURLException e) {
                        errorMessage = "Bad URL " + urlString;
                        log.error(errorMessage);
                    } catch (IOException e) {
                        errorMessage = "I/O error while downloading " + urlString + " because: " + e.getMessage();
                        log.error(errorMessage);
                    }
                }

                if (!installedOkay) {
                    logAndThrow("Failed to install module " + moduleId + ".  " + errorMessage);
                }
            }
        }

        try {
            FileWriter writer = new FileWriter(joinDirName(modulesDir, moduleId + EXT_PROPERTIES));
            Properties prop = new Properties();
            prop.put(PROP_VERSION, Version.version());
            prop.put(moduleId, properties.get(moduleId));
            prop.store(writer, "");
            log.info("Successfully installed module {}", moduleId);
        } catch (IOException e) {
            logAndThrow("Unable to write properties file for module " + moduleId + " because: " + e.getMessage(), e);
        }
    }

    public void upgrade(String moduleId) throws ModuleException {
        checkModuleInstalled(moduleId, true);
        log.info("Checking if module {} needs upgraded", moduleId);

        File file = new File(joinDirName(modulesDir, moduleId + EXT_PROPERTIES));
        try {
            FileReader reader = new FileReader(file);
            Properties prop = new Properties();
            prop.load(reader);
            String oldDepString = removeBlankSpace(prop.getProperty(moduleId));
            String newDepString = removeBlankSpace(properties.getProperty(moduleId));
            if (oldDepString == null || !oldDepString.equals(newDepString)) {
                log.info("Upgrading module {}", moduleId);
                remove(moduleId);
                install(moduleId);
            }
        } catch (IOException e) {
            logAndThrow("Unable to list files for module " + moduleId + " because: " + e.getMessage(), e);
        }
    }
    
    public void upgradeAll() throws ModuleException {
        for (String moduleId : list()) {
            upgrade(moduleId);
        }
    }

    public void convertToModules() {
        log.info("Module conversion starting");
        String dirName = System.getProperty(SystemConstants.SYSPROP_ENGINES_DIR, AppUtils.getSymHome() + "/engines");
        File dir = new File(dirName);
        File[] files = dir.listFiles();
        if (files != null ) {
            List<String> currentModules = list();
            log.info("Checking {} files in engines directory for possible module conversion", files.length);
            for (File file : files) {
                if (file.getName().endsWith(".properties")) {
                    log.info("Checking {} for possible module conversion", file.getPath());
                    convertToModule(file, currentModules);
                }
            }
        }
        log.info("Module conversion ended");
    }

    protected void convertToModule(File engineFile, List<String> currentModules) {
        TypedProperties prop = new TypedProperties();
        try (FileInputStream is = new FileInputStream(engineFile)) {
            prop.load(is);
        } catch (IOException e) {
            log.error("Failed module conversion for engine " + engineFile.getPath(), e);
            return;
        }

        String driver = prop.getProperty("db.driver");
        String moduleId = driverToModule.get(driver);
        if (moduleId != null && currentModules.contains(moduleId)) {
            log.info("Module '" + moduleId + "' already installed");
        } else if (moduleId != null && modules.containsKey(moduleId)) {
            try {
                install(moduleId);
                currentModules.add(moduleId);
            } catch (ModuleException e) {
                log.error("Failed module conversion for module " + moduleId, e);
            }
        } else {
            log.info("Skipping module conversion for driver '" + driver + "' and module '" + moduleId + "' for engine " + engineFile.getPath());
        }
    }

    private String buildUrl(String repo, MavenArtifact artifact, String extension) {
        return buildFileName(joinDirName(repo, artifact.getGroupId().replace(".", "/"), artifact.getArtifactId(),
                artifact.getVersion()), artifact, extension);
    }

    private String buildFileName(String baseDir, MavenArtifact artifact, String extension) {
        return joinDirName(baseDir, artifact.getArtifactId() + "-" + artifact.getVersion() + "." + extension);
    }

    private String joinDirName(String... args) {
        return StringUtils.join(args, "/");
    }

    private List<MavenArtifact> resolveArtifacts(String moduleId) {
        return modules.get(moduleId);
    }

    public void remove(String moduleId) throws ModuleException {
        checkModuleInstalled(moduleId, true);
        List<String> filesToRemove = listFiles(moduleId);
        for (String installedModuleId : list()) {
            if (!installedModuleId.equals(moduleId)) {
                filesToRemove.removeAll(listFiles(installedModuleId));
            }
        }

        boolean success = true;
        for (String fileName : filesToRemove) {
            boolean delSuccess = new File(joinDirName(modulesDir, fileName)).delete();
            log.info("Removing {} ({})", fileName, (delSuccess ? "OK" : "FAIL"));
            success &= delSuccess;
        }
        boolean delSuccess = new File(joinDirName(modulesDir, moduleId + EXT_PROPERTIES)).delete();
        log.info("Removing {} ({})", moduleId + EXT_PROPERTIES, (delSuccess ? "OK" : "FAIL"));
        success &= delSuccess;

        if (!success) {
            logAndThrow("Unable to remove all files associated with module " + moduleId);
        }
    }

    private String[] getPropFileNames() {
        File dir = new File(modulesDir);
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(EXT_PROPERTIES) && modules.containsKey(name.substring(0, name.indexOf(".")));
            }
        };
        return dir.list(filter);
    }

    public List<String> list() {
        List<String> names = new ArrayList<String>();
        String[] fileNames = getPropFileNames();
        if (fileNames != null) {
            for (String propFileName : getPropFileNames()) {
                names.add(propFileName.substring(0, propFileName.length() - EXT_PROPERTIES.length()));
            }
        }
        return names;
    }

    public List<String> listFiles(String moduleId) throws ModuleException {
        List<String> fileNames = new ArrayList<String>();
        checkModuleInstalled(moduleId, true);
        File file = new File(joinDirName(modulesDir, moduleId + EXT_PROPERTIES));
        if (file.canRead()) {
            try {
                FileReader reader = new FileReader(file);
                Properties prop = new Properties();
                prop.load(reader);
                for (MavenArtifact artifact : MavenArtifact.parseCsv(prop.getProperty(moduleId))) {
                    fileNames.add(artifact.toFileName(EXT_JAR));
                }
            } catch (IOException e) {
                logAndThrow("Unable to list files for module " + moduleId + " because: " + e.getMessage(), e);
            }
        }
        return fileNames;
    }

    public List<String> listDependencies(String moduleId) throws ModuleException {        
        checkModuleValid(moduleId);
        List<MavenArtifact> artifacts = resolveArtifacts(moduleId);
        List<String> fileNames = new ArrayList<String>();

        for (MavenArtifact artifact : artifacts) {
            fileNames.add(artifact.toFileName(EXT_JAR));
        }
        return fileNames;
    }

    public List<String> listAll() {
        List<String> names = new ArrayList<String>();
        for (String name : modules.keySet()) {
            names.add(name);
        }
        return names;
    }

    private void checkModuleInstalled(String moduleId, boolean shouldBeInstalled) throws ModuleException {
        checkModuleValid(moduleId);
        boolean isInstalled = list().contains(moduleId);
        if (isInstalled && !shouldBeInstalled) {
            throw new ModuleException("Module is already installed", false);
        } else if (!isInstalled && shouldBeInstalled) {
            throw new ModuleException("Module is not installed", false);
        }   
    }

    private void checkModuleValid(String moduleId) throws ModuleException {
        if (!modules.containsKey(moduleId)) {
            throw new ModuleException("Invalid module specified", false);
        }
    }

    private String removeBlankSpace(String str) {
        if (str != null) {
            str = str.replaceAll("\\s", "");
        }
        return str;
    }

    private void logAndThrow(String message) throws ModuleException {
        log.error(message);
        throw new ModuleException(message);
    }

    private void logAndThrow(String message, Exception e) throws ModuleException {
        log.error(message);
        throw new ModuleException(message, e);
    }

}
