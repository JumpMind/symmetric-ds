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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.IoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagedResource implements IStagedResource {

    static final Logger log = LoggerFactory.getLogger(StagedResource.class);
    
    private int references = 0;

    private File directory;
    
    private File file;

    private String path;

    private StringBuilder memoryBuffer;

    private long lastUpdateTime;

    private State state;
    
    private OutputStream outputStream = null;
    
    private Map<Thread, InputStream> inputStreams = null;
    
    private Map<Thread, BufferedReader> readers = null;

    private BufferedWriter writer;
    
    private StagingManager stagingManager;
    
    public StagedResource(File directory, String path, StagingManager stagingManager) {
        this.directory = directory;
        this.path = path;
        this.stagingManager = stagingManager;
        lastUpdateTime = System.currentTimeMillis();   
        
        if (buildFile(State.DONE).exists()){
            this.state = State.DONE;
        } else {
            this.state = State.CREATE;       
        }
        this.file = buildFile(state);
        if (file.exists()) {
            lastUpdateTime = file.lastModified();
        }
    }    
    
    protected static String toPath(File directory, File file) {
        String path = file.getAbsolutePath();
        path = path.replaceAll("\\\\", "/");
        path = path.substring(directory.getAbsolutePath().length(), file
                .getAbsolutePath().length());
        path = path.substring(1, path.lastIndexOf("."));
        return path;
    }
    
    @Override
    public void reference() {
        references++;
    }
    
    @Override
    public void dereference() {
        references--;
    }
    
    public boolean isInUse() {
        return references > 0 || (readers != null && readers.size() > 0) || writer != null || 
                (inputStreams != null && inputStreams.size() > 0) ||
                outputStream != null;
    }
    
    public boolean isFileResource() {     
        return file != null && file.exists();
    }

    protected File buildFile(State state) {
        return new File(directory, String.format("%s.%s", path, state.getExtensionName()));
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        if (file != null && file.exists()) {
            File newFile = buildFile(state);
            if (!newFile.equals(file)) {
                closeInternal();
                
                if (newFile.exists()) {
                    if (writer != null || outputStream != null) {
                        throw new IoException("Could not write '{}' it is currently being written to", newFile.getAbsolutePath());                                
                    }
                    
                    if (!FileUtils.deleteQuietly(newFile)) {
                        log.warn("Failed to delete '{}' in preparation for renaming '{}'", newFile.getAbsolutePath(), file.getAbsoluteFile());
                        if (readers != null && readers.size() > 0) {
                            for (Thread thread : readers.keySet()) {
                                BufferedReader reader = readers.get(thread);
                                log.warn("Closing unwanted reader for '{}' that had been created on thread '{}'", newFile.getAbsolutePath(), thread.getName());                                         
                                IOUtils.closeQuietly(reader);                                
                            }
                        }
                        readers = null;
                        
                        if (!FileUtils.deleteQuietly(newFile)) {
                            log.warn("Failed to delete '{}' for a second time", newFile.getAbsolutePath());
                        }
                    }
                }
                
                if (!file.renameTo(newFile)) {
                    String msg = String
                            .format("Had trouble renaming file.  The current name is %s and the desired state was %s",
                                    file.getAbsolutePath(), state);
                    log.warn(msg);
                    throw new IllegalStateException(msg);
                } 
            }
        } 
        
        refreshLastUpdateTime();
        this.state = state;
        this.file = buildFile(state);
    }

    public synchronized BufferedReader getReader() {
        Thread thread = Thread.currentThread();
        BufferedReader reader = readers != null ? readers.get(thread) : null;
        if (reader == null) {
            if (file != null && file.exists()) {
                try {
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                            IoConstants.ENCODING));
                    createReadersMap();
                    readers.put(thread, reader);
                } catch (IOException ex) {
                    throw new IoException(ex);
                }
            } else if (memoryBuffer != null && memoryBuffer.length() > 0) {
                reader = new BufferedReader(new StringReader(memoryBuffer.toString()));
                createReadersMap();
                readers.put(thread, reader);
            } else {
                throw new IllegalStateException(
                        "There is no content to read.  Memory buffer was empty and "
                                + file.getAbsolutePath() + " was not found.");
            }
        }
        return reader;
    }
    
    private synchronized final void createReadersMap() {
        if (readers == null) {
            readers = new HashMap<Thread, BufferedReader>(path.contains("common") ? 10 : 1);
        }
    }
    
    private synchronized final void closeReadersMap() {
        if (readers != null && readers.size() == 0) {
            readers = null;
        }
    }
    
    private synchronized final void createInputStreamsMap() {
        if (inputStreams == null) {
            inputStreams = new HashMap<Thread, InputStream>(path.contains("common") ? 10 : 1);
        }
    }
    
    private synchronized final void closeInputStreamsMap() {
        if (inputStreams != null && inputStreams.size() == 0) {
            inputStreams = null;
        }
    }    
    
    public void close() {
        closeInternal();
        if (isFileResource()) {
            stagingManager.inUse.remove(path);
        }
    }
    
    private void closeInternal() {
        Thread thread = Thread.currentThread();
        BufferedReader reader = readers != null ? readers.get(thread) : null;
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            readers.remove(thread);
            closeReadersMap();
        }

        if (writer != null) {
            IOUtils.closeQuietly(writer);
            writer = null;
        }
        
        if (outputStream != null) {
            IOUtils.closeQuietly(outputStream);
            outputStream = null;
        }
        
        InputStream inputStream = inputStreams != null ? inputStreams.get(thread) : null;
        if (inputStream != null) {
            IOUtils.closeQuietly(inputStream);
            inputStreams.remove(thread);
            closeInputStreamsMap();
        }
    }
    
    public OutputStream getOutputStream() {
        try {            
            if (outputStream == null) {
                if (file != null && file.exists()) {
                    log.warn("We had to delete {} because it already existed",
                            file.getAbsolutePath());
                    file.delete();
                }
                file.getParentFile().mkdirs();
                outputStream = new BufferedOutputStream(new FileOutputStream(file));
            }
            return outputStream;
        } catch (FileNotFoundException e) {
            throw new IoException(e);
        }
    }

    public synchronized InputStream getInputStream() {
        Thread thread = Thread.currentThread();
        InputStream reader = inputStreams != null ? inputStreams.get(thread) : null;
        if (reader == null) {
            if (file != null && file.exists()) {
                try {
                    reader = new BufferedInputStream(new FileInputStream(file));
                    createInputStreamsMap();
                    inputStreams.put(thread, reader);
                } catch (IOException ex) {
                    throw new IoException(ex);
                }
            } else {
                throw new IllegalStateException("There is no content to read. "
                        + file.getAbsolutePath() + " was not found.");
            }
        }
        return reader;
    }
    
    public BufferedWriter getWriter(long threshold) {
        if (writer == null) {
            if (file != null && file.exists()) {
                log.warn("We had to delete {} because it already existed", file.getAbsolutePath());
                file.delete();
            } else if (this.memoryBuffer != null) {
                log.warn("We had to delete the memory buffer for {} because it already existed", getPath());
                this.memoryBuffer = null;
            }
            this.memoryBuffer = threshold > 0 ? new StringBuilder() : null;
            writer = new BufferedWriter(new ThresholdFileWriter(threshold, this.memoryBuffer,
                    file));
        }
        return writer;
    }

    public long getSize() {
        if (file != null && file.exists()) {
            return file.length();
        } else if (memoryBuffer != null) {
            return memoryBuffer.length();
        } else {
            return 0;
        }
    }

    public boolean exists() {
        return (file != null && file.exists() && file.length() > 0) || (memoryBuffer != null && memoryBuffer.length() > 0);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void refreshLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public boolean delete() {        
        close();
        return deleteInternal();
    }
    
    private boolean deleteInternal() {
        boolean deleted = false;
        if (file != null && file.exists()) {
            FileUtils.deleteQuietly(file);
            deleted = !file.exists();
        }

        if (memoryBuffer != null) {
            memoryBuffer = null;
            deleted = true;
        }

        if (deleted) {
            stagingManager.resourcePaths.remove(path);
            stagingManager.inUse.remove(path);
        }        
        return deleted;
    }

    public File getFile() {
        return file;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return (file != null && file.exists()) ? file.getAbsolutePath() : String.format("%d bytes in memory",
                memoryBuffer != null ? memoryBuffer.length() : 0);
    }

}
