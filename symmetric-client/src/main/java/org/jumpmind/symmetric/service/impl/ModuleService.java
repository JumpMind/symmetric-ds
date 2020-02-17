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

package org.jumpmind.symmetric.service.impl;

import java.io.File;
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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IModuleService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.MavenArtifact;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModuleService extends AbstractService implements IModuleService {

    private final static Logger log = LoggerFactory.getLogger(ModuleService.class);

    public static final String REGEX_CSV = "\\s*,\\s*";

    private static final String EXT_PROPERTIES = ".properties";

    private static final String EXT_JAR = "jar";

    private static final String PROP_REPOSITORIES = "repos";

    private static final String PROP_DEPENDENCIES = "dependencies";

    private static final String PROP_VERSION = "sym.version";

    private Map<String, List<MavenArtifact>> modules = new HashMap<String, List<MavenArtifact>>();

    private List<String> repos = new ArrayList<String>();
    
    private String modulesDir;

    public ModuleService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);

        modulesDir = joinDirName(AppUtils.getSymHome(), "lib");

        try (InputStream in = getClass().getResourceAsStream("/symmetric-modules.properties")) {
            Properties prop = new Properties();
            prop.load(in);

            for (Entry<Object, Object> entry : prop.entrySet()) {
                String key = entry.getKey().toString();
                if (key.equals(PROP_REPOSITORIES)) {
                    for (String repo : entry.getValue().toString().split(REGEX_CSV)) {
                        repos.add(repo);
                    }
                } else {
                    List<MavenArtifact> list = new ArrayList<MavenArtifact>();
                    for (String dependency : entry.getValue().toString().split(REGEX_CSV)) {
                        list.add(new MavenArtifact(dependency));
                    }
                    modules.put(key, list);
                }
            }
        } catch (Exception e) {
            log.error("Unable to read symmetric-modules.properties file: {}", e.getMessage());
        }
    }

    @Override
    public boolean install(String moduleId) {
        boolean installedOkay = false;
        List<MavenArtifact> artifacts = resolveArtifacts(moduleId);
        if (artifacts == null) {
            log.error("Invalid module specified");
            return false;
        }

        log.info("Installing module {} with {} artifacts", moduleId, artifacts.size());

        for (MavenArtifact artifact : artifacts) {
            String fileName = buildFileName(modulesDir, artifact, EXT_JAR);
            if (new File(fileName).exists()) {
                installedOkay = true;
                log.info("{} already exists", fileName);
            } else {
                for (String repo : repos) {
                    String urlString = buildUrl(repo, artifact, EXT_JAR);

                    try {
                        URL url = new URL(urlString);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("GET");
                        conn.setInstanceFollowRedirects(true);
                        conn.setConnectTimeout(10000);
                        conn.setReadTimeout(60000);

                        if (conn.getResponseCode() == 200) {
                            log.info("Downloading {}", urlString);

                            try (InputStream in = conn.getInputStream();
                                    FileOutputStream out = new FileOutputStream(fileName)) {
                                IOUtils.copy(in, out);
                                installedOkay = true;
                                break;
                            } catch (Exception e) {
                                log.error("Unable to download from {} because: {}", urlString, e.getMessage());
                            }
                        } else {
                            log.debug("Unable to download [{}] from {}", conn.getResponseCode(), urlString);
                        }
                    } catch (MalformedURLException e) {
                        log.error("Bad URL {}", urlString);
                    } catch (IOException e) {
                        log.error("I/O error while downloading {} because: {}", urlString, e.getMessage());
                    }
                }
            }

            if (!installedOkay) {
                log.error("Failed to install module {}", moduleId);
                break;
            }
        }

        if (installedOkay) {
            try {
                FileWriter writer = new FileWriter(joinDirName(modulesDir, moduleId + EXT_PROPERTIES));
                Properties prop = new Properties();
                prop.put(PROP_VERSION, Version.version());
                StringBuilder sb = new StringBuilder();
                for (MavenArtifact artifact : artifacts) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(artifact.toString());
                }
                prop.put(PROP_DEPENDENCIES, sb.toString());
                prop.store(writer, "");
                log.info("Successfully installed module {}", moduleId);
            } catch (IOException e) {
                log.error("Unable to write properties file for module {} because: {}", moduleId, e.getMessage());
            }
        }

        return installedOkay;
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

    @Override
    public boolean remove(String moduleId) {
        List<String> filesToRemove = listFiles(moduleId);
        for (String installedModuleId : list()) {
            if (!installedModuleId.equals(moduleId)) {
                filesToRemove.removeAll(listFiles(installedModuleId));
            }
        }

        boolean success = true;
        for (String fileName : filesToRemove) {
            success &= new File(joinDirName(modulesDir, fileName)).delete();
        }
        success &= new File(joinDirName(modulesDir, moduleId + EXT_PROPERTIES)).delete();
        return success;
    }

    private String[] getPropFileNames() {
        File dir = new File(modulesDir);
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(EXT_PROPERTIES);
            }
        };
        return dir.list(filter);
    }

    @Override
    public List<String> list() {
        List<String> names = new ArrayList<String>();
        for (String propFileName : getPropFileNames()) {
            names.add(propFileName.substring(0, propFileName.length() - EXT_PROPERTIES.length()));
        }
        return names;
    }

    @Override
    public List<String> listFiles(String moduleId) {
        List<String> fileNames = new ArrayList<String>();
        if (modules.containsKey(moduleId)) {
            File file = new File(joinDirName(modulesDir, moduleId + EXT_PROPERTIES));
            if (file.canRead()) {
                try {
                    FileReader reader = new FileReader(file);
                    Properties prop = new Properties();
                    prop.load(reader);
                    for (MavenArtifact artifact : MavenArtifact.parseCsv(prop.getProperty(PROP_DEPENDENCIES))) {
                        fileNames.add(artifact.toFileName(EXT_JAR));
                    }
                } catch (IOException e) {
                    log.error("Unable to list files for module '{}' because: ", moduleId, e.getMessage());
                }
            }
        }
        return fileNames;
    }

    @Override
    public List<String> listAll() {
        List<String> names = new ArrayList<String>();
        for (String name : modules.keySet()) {
            names.add(name);
        }
        return names;
    }

}
