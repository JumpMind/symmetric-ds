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
package org.jumpmind.symmetric.io.stage;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingManager implements IStagingManager {

    protected static final Logger log = LoggerFactory.getLogger(StagingManager.class);

    protected File directory;
    
    protected Set<String> resourcePaths;
    
    protected Map<String, IStagedResource> inUse;
    
    boolean clusterEnabled;

    public StagingManager(String directory, boolean clusterEnabled) {
        log.info("The staging directory was initialized at the following location: " + directory);
        this.directory = new File(directory);
        this.directory.mkdirs();
        this.resourcePaths = Collections.synchronizedSet(new TreeSet<String>());
        this.inUse = new ConcurrentHashMap<String, IStagedResource>();
        this.clusterEnabled = clusterEnabled;
        refreshResourceList();
    }

    public Set<String> getResourceReferences() {
        synchronized (resourcePaths) {
            return new TreeSet<String>(resourcePaths);
        }
    }

    private void refreshResourceList() {
        Collection<File> files = FileUtils.listFiles(this.directory,
                new String[] { State.CREATE.getExtensionName(), State.DONE.getExtensionName(), State.DONE.getExtensionName() }, true);
        for (File file : files) {
            try {
                String path = StagedResource.toPath(directory, file);
                if (!resourcePaths.contains(path)) {
                    resourcePaths.add(path);
                }
            } catch (IllegalStateException ex) {
                log.warn(ex.getMessage());
            }
        }
    }

    /**
     * Clean up resources that are older than the passed in parameter.
     * 
     * @param ttlInMs
     *            If resources are older than this number of milliseconds they
     *            will be purged
     */
    public long clean(long ttlInMs) {
        synchronized (StagingManager.class) {
            log.trace("Cleaning staging area");
            Set<String> keys = getResourceReferences();
            long purgedFileCount = 0;
            long purgedFileSize = 0;
            long purgedMemCount = 0;
            long purgedMemSize = 0;
            for (String key : keys) {
                IStagedResource resource = new StagedResource(directory, key, this);
                /* resource could have deleted itself between the time the keys were cloned and now */
                if (resource != null) {
                    boolean resourceIsOld = (System.currentTimeMillis() - resource
                            .getLastUpdateTime()) > ttlInMs;
                    if (resource.getState() == State.DONE && resourceIsOld) {
                        if (!resource.isInUse()) {
                            boolean file = resource.isFileResource();
                            long size = resource.getSize();
                            if (resource.delete()) {
                                if (file) {
                                    purgedFileCount++;
                                    purgedFileSize += size;
                                } else {
                                    purgedMemCount++;
                                    purgedMemSize += size;
                                }
                                resourcePaths.remove(key);
                            } else {
                                log.warn("Failed to delete the '{}' staging resource",
                                        resource.getPath());
                            }
                        } else {
                            log.info(
                                    "The '{}' staging resource qualified for being cleaned, but was in use.  It will not be cleaned right now",
                                    resource.getPath());
                        }
                    }
                }
            }
            if (purgedFileCount > 0) {
                if (purgedFileSize < 1000) {
                    log.debug("Purged {} staged files, freeing {} bytes of disk space",
                            purgedFileCount, (int) (purgedFileSize));
                } else {
                    log.debug("Purged {} staged files, freeing {} kbytes of disk space",
                            purgedFileCount, (int) (purgedFileSize / 1000));
                }
            }
            if (purgedMemCount > 0) {
                if (purgedMemSize < 1000) {
                    log.debug("Purged {} staged memory buffers, freeing {} bytes of memory",
                            purgedMemCount, (int) (purgedMemSize));
                } else {
                    log.debug("Purged {} staged memory buffers, freeing {} kbytes of memory",
                            purgedMemCount, (int) (purgedMemSize / 1000));
                }
            }
            return purgedFileCount + purgedMemCount;
        }
    }
    
    /**
     * Create a handle that can be written to
     */
    public IStagedResource create(Object... path) {
        String filePath = buildFilePath(path);
        IStagedResource resource = createStagedResource(filePath);
        if (resource.exists()) {
            resource.delete();
        }
        this.inUse.put(filePath, resource);
        this.resourcePaths.add(filePath);
        return resource;
    }
    
    protected IStagedResource createStagedResource(String filePath) {
    	return new StagedResource(directory, filePath, this);    	
    }

    protected String buildFilePath(Object... path) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            Object part = path[i];
            if (part instanceof Number) {
                part = StringUtils.leftPad(part.toString(), 10, "0");
            }
            buffer.append(part);
            if (i < path.length - 1) {
                buffer.append("/");
            }
        }
        return buffer.toString();
    }
    
    public IStagedResource find(String path) {
        IStagedResource resource = inUse.get(path);
        if (resource == null) {
            boolean foundResourcePath = resourcePaths.contains(path);
            if (!foundResourcePath && clusterEnabled) {
                refreshResourceList();
                foundResourcePath = resourcePaths.contains(path);
            }        
            if (foundResourcePath) {
                resource = new StagedResource(directory, path, this);         
            }
        }
        return resource;
    }

    public IStagedResource find(Object... path) {
        return find(buildFilePath(path));
    }

}
