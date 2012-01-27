package org.jumpmind.symmetric.io.stage;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingManager implements IStagingManager {

    protected static final Logger log = LoggerFactory.getLogger(StagingManager.class);

    protected long timeToLiveInMs;

    protected File directory;

    protected long memoryThresholdInBytes;

    protected Map<String, IStagedResource> resourceList = new HashMap<String, IStagedResource>();

    public StagingManager(long memoryThresholdInBytes, long timeToLiveInMs, String directory) {
        this.timeToLiveInMs = timeToLiveInMs;
        this.memoryThresholdInBytes = memoryThresholdInBytes;
        this.directory = new File(directory);
        this.directory.mkdirs();
        refreshResourceList();
    }

    protected void refreshResourceList() {

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
            StagedResource resource = new StagedResource(memoryThresholdInBytes, directory, file);
            resourceList.put(resource.getPath(), resource);
        }

    }

    /**
     * Clean up files that are older than {@link #timeToLiveInMs} and have been
     * marked as done.
     */
    public void clean() {
        this.refreshResourceList();
        Set<String> keys = new HashSet<String>(resourceList.keySet());
        int purgedCount = 0;
        long purgedSize = 0;
        for (String key : keys) {
            IStagedResource resource = resourceList.get(key);
            if (resource.getState() == State.DONE
                    && (System.currentTimeMillis() - resource.getCreateTime()) > timeToLiveInMs) {
                purgedCount++;
                purgedSize += resource.getSize();
                resource.delete();
                resourceList.remove(key);
            }
        }
        log.info("Purged {} staged files, freeing {} kb of disk space", purgedCount, (int)(purgedSize/1000));
    }

    /**
     * Create a handle that can be written to
     */
    public IStagedResource create(Object... path) {
        String filePath = buildFilePath(path);
        StagedResource resource = new StagedResource(memoryThresholdInBytes, directory, filePath);
        this.resourceList.put(filePath, resource);
        return resource;
    }
    
    protected String buildFilePath(Object... path) {
        StringBuilder buffer = new StringBuilder();
        for(int i = 0; i < path.length; i++) {
            buffer.append(path[i]);
            if (i < path.length-1) {
                buffer.append(System.getProperty("file.separator"));
            } 
        }
        return buffer.toString();
    }

    public IStagedResource find(Object... path) {
        String filePath = buildFilePath(path);
        return resourceList.get(filePath);
    }

}
