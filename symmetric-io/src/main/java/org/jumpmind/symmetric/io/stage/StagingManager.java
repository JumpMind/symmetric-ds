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
package org.jumpmind.symmetric.io.stage;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingManager implements IStagingManager {

    protected static final Logger log = LoggerFactory.getLogger(StagingManager.class);

    protected long timeToLiveInMs;

    protected File directory;

    protected long memoryThresholdInBytes;

    protected Map<String, IStagedResource> resourceList = new ConcurrentHashMap<String, IStagedResource>();

    public StagingManager(long memoryThresholdInBytes, long timeToLiveInMs, String directory) {
        this.timeToLiveInMs = timeToLiveInMs;
        this.memoryThresholdInBytes = memoryThresholdInBytes;
        this.directory = new File(directory);
        this.directory.mkdirs();
        refreshResourceList();
    }

    protected void refreshResourceList() {
        synchronized (StagingManager.class) {
            Set<String> keys = new HashSet<String>(resourceList.keySet());
            for (String key : keys) {
                IStagedResource resource = resourceList.get(key);
                if (resource != null && !resource.exists()) {
                    resourceList.remove(key);
                }
            }

            Collection<File> files = FileUtils.listFiles(this.directory,
                    new String[] { State.CREATE.getExtensionName(), State.READY.getExtensionName(),
                            State.DONE.getExtensionName() }, true);
            for (File file : files) {
                try {
                    StagedResource resource = new StagedResource(memoryThresholdInBytes, directory,
                            file, this);
                    resourceList.put(resource.getPath(), resource);
                } catch (IllegalStateException ex) {
                    log.warn(ex.getMessage());
                }
            }
        }
    }

    /**
     * Clean up files that are older than {@link #timeToLiveInMs} and have been
     * marked as done.
     */
    public long clean() {
        synchronized (StagingManager.class) {
            this.refreshResourceList();

            Set<String> keys = new HashSet<String>(resourceList.keySet());
            long purgedFileCount = 0;
            long purgedFileSize = 0;
            long purgedMemCount = 0;
            long purgedMemSize = 0;
            for (String key : keys) {
                IStagedResource resource = resourceList.get(key);
                boolean resourceIsOld = (System.currentTimeMillis() - resource.getCreateTime()) > timeToLiveInMs;
                if ((resource.getState() == State.READY || resource.getState() == State.DONE)
                        && (resourceIsOld || !resource.exists())) {
                    if (resource.isFileResource()) {
                        purgedFileCount++;
                        purgedFileSize += resource.getSize();
                    } else {
                        purgedMemCount++;
                        purgedMemSize += resource.getSize();
                    }
                    resource.delete();
                    resourceList.remove(key);
                }
            }
            if (purgedFileCount > 0) {
                log.info("Purged {} staged files, freeing {} kb of disk space", purgedFileCount,
                        (int) (purgedFileSize / 1000));
            }
            if (purgedMemCount > 0) {
                log.info("Purged {} staged memory buffers, freeing {} kb of memory",
                        purgedMemCount, (int) (purgedMemSize / 1000));
            }
            return purgedFileCount + purgedMemCount;
        }
    }

    /**
     * Create a handle that can be written to
     */
    public IStagedResource create(Object... path) {
        String filePath = buildFilePath(path);
        StagedResource resource = new StagedResource(memoryThresholdInBytes, directory, filePath,
                this);
        this.resourceList.put(filePath, resource);
        return resource;
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
                buffer.append(System.getProperty("file.separator"));
            }
        }
        return buffer.toString();
    }

    public IStagedResource find(Object... path) {
        String filePath = buildFilePath(path);
        IStagedResource resource = resourceList.get(filePath);
        if (resource != null) {
            if (!resource.exists()
                    && (resource.getState() == State.READY || resource.getState() == State.DONE)) {
                resource.delete();
                resource = null;
            }
        }

        return resource;
    }

}
