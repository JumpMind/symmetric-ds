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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingManager implements IStagingManager {

    private static final String LOCK_EXTENSION = ".lock";

    protected static final Logger log = LoggerFactory.getLogger(StagingManager.class);

    protected File directory;

    private Map<String, String> resourcePathsCache = new ConcurrentHashMap<String, String>();
    protected Map<String, IStagedResource> inUse = new ConcurrentHashMap<String, IStagedResource>();

    boolean clusterEnabled;

    public StagingManager(String directory, boolean clusterEnabled) {
        log.info("The staging directory was initialized at the following location: " + directory);
        this.directory = new File(directory);
        this.directory.mkdirs();
        this.clusterEnabled = clusterEnabled;
    }

    @Override
    public Set<String> getResourceReferences() {
        return new TreeSet<String>(resourcePathsCache.keySet());
    }

    @Override
    public long clean(long ttlInMs) {
        return clean(ttlInMs, null);
    }
    
    public synchronized long clean(long ttlInMs, StagingPurgeContext context) {
        try {
            log.info("Cleaning staging...");
            if (context == null) {
                context = new StagingPurgeContext();
            }
            long start = System.currentTimeMillis();
            context.setStartTime(start);
            
            resourcePathsCache.clear();
            clean(FileSystems.getDefault().getPath(this.directory.getAbsolutePath()), ttlInMs, context);
            logCleaningProgress(context);
            long end = System.currentTimeMillis();
            log.info("Finished cleaning staging in " + DurationFormatUtils.formatDurationWords(end-start, true, true) + ".");
            return context.getPurgedFileSize() + context.getPurgedMemSize();
        } catch (Exception ex) {
            throw new RuntimeException("Failure while cleaning staging.", ex);
        }
    }

    protected void logCleaningProgress(StagingPurgeContext context) {
        if (context.getPurgedFileCount() > 0) {
            log.info("Purged {} staging files, freed {} of disk space.",
                    context.getPurgedFileCount(), FileUtils.byteCountToDisplaySize(context.getPurgedFileSize()));
        }
        if (context.getPurgedMemCount() > 0) {
            log.info("Purged {} staging memory buffers, freed {}.",
                    context.getPurgedMemCount(), FileUtils.byteCountToDisplaySize(context.getPurgedMemSize()));                    
        }
    }

    protected void clean(Path path, long ttlInMs, StagingPurgeContext context) throws IOException {
        DirectoryStream<Path> stream = Files.newDirectoryStream(path, STAGING_FILE_FILTER);
        
        if (context.shouldLogStatus()) {
            logCleaningProgress(context);
            context.setLastLogTime(System.currentTimeMillis());
        }
        
        for (Path entry : stream) {
            if (Files.isDirectory(entry)) {
                clean(entry, ttlInMs, context);
            } else {     
                try {
                    String parentDirectory = "";
                    if (entry.getParent() != null) {
                        parentDirectory = entry.getParent().toString();
                    }
                    String entryName = "";
                    if ( entry.getFileName() != null) {
                        entryName = entry.getFileName().toString();
                    }
                    String stagingPath = StagedResource.toPath(directory, 
                            new File((parentDirectory + "/" + entryName)));

                    IStagedResource resource = createStagedResource(stagingPath);  
                    if (stagingPath != null) {
                        if (shouldCleanPath(resource, ttlInMs, context)) {
                            if (resource.getFile() != null) {
                                context.incrementPurgedFileCount();
                                context.addPurgedFileBytes(resource.getSize());
                            } else {
                                context.incrementPurgedMemoryCount();
                                context.addPurgedMemoryBytes(resource.getSize());
                            }
                            
                            cleanPath(resource, ttlInMs, context); // this comes after stat collection because 
                                                                   // once the file is gone we loose visibility to size
                        } else {
                            resourcePathsCache.put(stagingPath,stagingPath);                            
                        }
                    }
                } catch (IllegalStateException ex) {
                    log.warn("Failure during refreshResourceList ", ex);
                }                
            }
        }

        stream.close();
    } 
    
    protected boolean shouldCleanPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context) {
        boolean resourceIsOld = (System.currentTimeMillis() - resource.getLastUpdateTime()) > ttlInMs;
        return (resourceIsOld && resource.getState() == State.DONE && !resource.isInUse());
    }

    protected boolean cleanPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context) {
        boolean success = resource.delete(); 
        if (!success) {
            log.warn("Failed to delete the '{}' staging resource", resource.getPath());
        }
        return success;
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
        this.resourcePathsCache.put(filePath, filePath);
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
            boolean foundResourcePath = resourcePathsCache.containsKey(path);
            if (!foundResourcePath && clusterEnabled) {
                synchronized (this) {
                    IStagedResource staged = createStagedResource(path);
                    if (staged.exists() && staged.getState() == State.DONE) {
                        resourcePathsCache.put(path, path);
                        resource = staged;
                        foundResourcePath = true;
                    }
                }
            } else if (foundResourcePath) {
                resource = createStagedResource(path);           
            }
        }
        return resource;
    }

    public IStagedResource find(Object... path) {
        return find(buildFilePath(path));
    }

    public void removeResourcePath(String path) {
        resourcePathsCache.remove(path);
        inUse.remove(path);
    }        

    @Override
    public StagingFileLock acquireFileLock(String serverInfo, Object... path) {
        String lockFilePath = String.format("%s/%s%s", directory, buildFilePath(path), LOCK_EXTENSION);
        log.debug("About to acquire lock at {}", lockFilePath);

        StagingFileLock stagingFileLock = new StagingFileLock();

        File lockFile = new File(lockFilePath);
        File containingDirectory = lockFile.getParentFile();

        if (containingDirectory != null) {
            containingDirectory.mkdirs();
        }        

        boolean acquired = false;
        try {
            acquired = lockFile.createNewFile();
            if (acquired) {
                FileUtils.write(lockFile, serverInfo);
            }
        } catch (IOException ex) { // Hitting this when file already exists.
            log.debug("Failed to create lock file  (" + lockFilePath + ")", ex);
        }

        stagingFileLock.setAcquired(acquired);
        stagingFileLock.setLockFile(lockFile);

        if (!acquired) {
            if (lockFile.exists()) {
                try {                    
                    String lockFileContents = FileUtils.readFileToString(lockFile, "UTF8");
                    stagingFileLock.setLockFailureMessage("Lock file exists: " + lockFileContents);
                } catch (Exception ex) {
                    stagingFileLock.setLockFailureMessage("Lock file exists but could not read contents: " + ex.getMessage());
                    if (log.isDebugEnabled()) {                        
                        log.debug("Failed to read lock file contents (" + lockFilePath + ")", ex);
                    }
                }
            } else {
                stagingFileLock.setLockFailureMessage("Lock file does not exist, but could not be created. Check directory permissions.");
            }
        }


        return stagingFileLock;
    }

    protected static final DirectoryStream.Filter<Path> STAGING_FILE_FILTER = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(Path entry) {
            try {
                boolean accept = Files.isDirectory(entry) ||
                    entry.getFileName().toString().endsWith(".create")
                    || entry.getFileName().toString().endsWith(".done");
                return accept;
            } catch (NullPointerException ex ) {
                return false;
            }
        }
    };


}
