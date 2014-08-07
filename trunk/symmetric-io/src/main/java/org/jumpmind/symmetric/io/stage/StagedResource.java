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

    private long threshold;

    private File directory;

    private String path;

    private File file;

    private StringBuilder memoryBuffer;

    private long lastUpdateTime;

    private State state;
    
    private OutputStream outputStream = null;
    
    private Map<Thread, InputStream> inputStreams = null;
    
    private Map<Thread, BufferedReader> readers = new HashMap<Thread, BufferedReader>();

    private BufferedWriter writer;
    
    private StagingManager stagingManager;

    public StagedResource(long threshold, File directory, File file, StagingManager stagingManager) {
        this.threshold = threshold;
        this.directory = directory;
        this.stagingManager = stagingManager;
        this.file = file;
        this.path = file.getAbsolutePath();
        this.path = this.path.replaceAll("\\\\", "/");
        this.path = this.path.substring(directory.getAbsolutePath().length(), file
                .getAbsolutePath().length());
        this.path = this.path.substring(1, path.lastIndexOf("."));
        if (file.exists()) {
            lastUpdateTime = file.lastModified();
            String fileName = file.getName();
            String extension = fileName.substring(fileName.lastIndexOf(".") + 1, fileName.length());
            this.state = State.valueOf(extension.toUpperCase());
        } else {
            throw new IllegalStateException(String.format("The passed in file, %s, does not exist",
                    file.getAbsolutePath()));
        }
    }

    public StagedResource(long threshold, File directory, String path, StagingManager stagingManager) {
        this.threshold = threshold;
        this.directory = directory;
        this.path = path;
        this.stagingManager = stagingManager;
        this.file = new File(directory, String.format("%s.%s", path,
                State.CREATE.getExtensionName()));
        lastUpdateTime = System.currentTimeMillis();
        this.state = State.CREATE;
    }
    
    public boolean isInUse() {
        return readers.size() > 0 || writer != null || 
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
        if (file.exists()) {
            File newFile = buildFile(state);
            if (!newFile.equals(file)) {
                if (newFile.exists()) {
                    if (writer != null || outputStream != null) {
                        throw new IoException("Could not write '{}' it is currently being written to", newFile.getAbsolutePath());                                
                    }
                    
                    if (!FileUtils.deleteQuietly(newFile)) {
                        log.warn("Failed to delete '{}' in preparation for renaming '{}'", newFile.getAbsolutePath(), file.getAbsoluteFile());
                        if (readers.size() > 0) {
                            for (Thread thread : readers.keySet()) {
                                BufferedReader reader = readers.get(thread);
                                log.warn("Closing unwanted reader for '{}' that had been created on thread '{}'", newFile.getAbsolutePath(), thread.getName());                                         
                                IOUtils.closeQuietly(reader);                                
                            }
                        }
                        
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
                } else {
                    this.file = newFile;
                }
            }
        } else if (memoryBuffer != null && state == State.DONE) {
            this.memoryBuffer.setLength(0);
            this.memoryBuffer = null;
        }
        refreshLastUpdateTime();
        this.state = state;
    }

    public BufferedReader getReader() {
        Thread thread = Thread.currentThread();
        BufferedReader reader = readers.get(thread);
        if (reader == null) {
            if (file.exists()) {
                try {
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(file),
                            IoConstants.ENCODING));
                    readers.put(thread, reader);
                } catch (IOException ex) {
                    throw new IoException(ex);
                }
            } else if (memoryBuffer != null && memoryBuffer.length() > 0) {
                reader = new BufferedReader(new StringReader(memoryBuffer.toString()));
                readers.put(thread, reader);
            } else {
                throw new IllegalStateException(
                        "There is no content to read.  Memory buffer was empty and "
                                + file.getAbsolutePath() + " was not found.");
            }
        }
        return reader;
    }

    public void close() {
        Thread thread = Thread.currentThread();
        BufferedReader reader = readers.get(thread);
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            readers.remove(thread);
        }

        if (writer != null) {
            IOUtils.closeQuietly(writer);
        }
        
        if (outputStream != null) {
            IOUtils.closeQuietly(outputStream);
        }
        
        if (inputStreams != null) {
            InputStream inputStream = inputStreams.get(thread);
            if (inputStream != null) {
                IOUtils.closeQuietly(inputStream);
                inputStreams.remove(thread);
            }
        }
    }
    
    public OutputStream getOutputStream() {
        try {            
            if (outputStream == null) {
                if (file.exists()) {
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

    public InputStream getInputStream() {
        Thread thread = Thread.currentThread();
        if (inputStreams == null) {
            inputStreams = new HashMap<Thread, InputStream>();
        }
        InputStream reader = inputStreams.get(thread);
        if (reader == null) {
            if (file.exists()) {
                try {
                    reader = new BufferedInputStream(new FileInputStream(file));
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
    
    public BufferedWriter getWriter() {
        if (writer == null) {
            if (file.exists()) {
                log.warn("We had to delete {} because it already existed", file.getAbsolutePath());
                file.delete();
            } else if (this.memoryBuffer != null) {
                log.warn("We had to delete the memory buffer for {} because it already existed", getPath());
                this.memoryBuffer = null;
            }
            this.memoryBuffer = new StringBuilder();
            writer = new BufferedWriter(new ThresholdFileWriter(threshold, this.memoryBuffer,
                    this.file));
        }
        return writer;
    }

    public long getSize() {
        if (file.exists()) {
            return file.length();
        } else if (memoryBuffer != null) {
            return memoryBuffer.length();
        } else {
            return 0;
        }
    }

    public boolean exists() {
        return (file.exists() && file.length() > 0) || (memoryBuffer != null && memoryBuffer.length() > 0);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void refreshLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public boolean delete() {
        
        boolean deleted = true;
        
        close();
        
        if (file.exists()) {
            FileUtils.deleteQuietly(file);
            deleted = !file.exists();            
        }

        if (memoryBuffer != null) {
            memoryBuffer.setLength(0);
            memoryBuffer = null;
        }
        
        if (deleted) {
            stagingManager.resourceList.remove(getPath());
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
        return file.exists() ? file.getAbsolutePath() : String.format("%d bytes in memory",
                memoryBuffer.length());
    }

}
