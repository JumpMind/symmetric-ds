package org.jumpmind.symmetric.io.stage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagedResource implements IStagedResource {

    static final Logger log = LoggerFactory.getLogger(StagedResource.class);

    private long threshold;

    private File directory;

    private String path;

    private File file;

    private StringBuilder memoryBuffer;

    private long createTime;

    private State state;

    private BufferedReader reader;

    private BufferedWriter writer;

    public StagedResource(long threshold, File directory, File file) {
        this.threshold = threshold;
        this.directory = directory;
        this.file = file;
        this.path = file.getAbsolutePath();
        this.path = this.path.substring(directory.getAbsolutePath().length(), file
                .getAbsolutePath().length());
        this.path = this.path.substring(0, path.lastIndexOf("."));
        if (file.exists()) {
            createTime = file.lastModified();
            String fileName = file.getName();
            String extension = fileName.substring(fileName.lastIndexOf(".")+1, fileName.length());
            this.state = State.valueOf(extension.toUpperCase());
        } else {
            throw new IllegalStateException(String.format("The passed in file, %s, does not exist",
                    file.getAbsolutePath()));
        }
    }

    public StagedResource(long threshold, File directory, String path) {
        this.threshold = threshold;
        this.directory = directory;
        this.path = path;
        this.file = new File(directory, String.format("%s.%s", path,
                State.CREATE.getExtensionName()));
        createTime = System.currentTimeMillis();
        this.state = State.CREATE;
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
        this.state = state;
    }

    public BufferedReader getReader() {
        if (reader == null) {
            if (file.exists()) {
                try {
                    this.reader = new BufferedReader(new InputStreamReader(
                            new FileInputStream(file), "UTF-8"));
                } catch (IOException ex) {
                    throw new IoException(ex);
                }
            } else if (memoryBuffer != null && memoryBuffer.length() > 0) {
                this.reader = new BufferedReader(new StringReader(memoryBuffer.toString()));
            } else {
                throw new IllegalStateException("There is no content to read");
            }
        }
        return reader;
    }

    public void close() {
        IOUtils.closeQuietly(reader);
        reader = null;
        IOUtils.closeQuietly(writer);
        writer = null;
    }

    public BufferedWriter getWriter() {
        if (writer == null) {
            if (file.exists()) {
                throw new IllegalStateException(String.format(
                        "Cannot create a writer because the file, %s, already exists", file.getAbsoluteFile()));
            } else if (this.memoryBuffer != null) {
                throw new IllegalStateException(
                        "Cannot create a writer because the memory buffer has already been written to.");
            }
            this.memoryBuffer = new StringBuilder();
            this.writer = new BufferedWriter(new ThresholdFileWriter(threshold, this.memoryBuffer,
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
        return file.exists() || (memoryBuffer != null && memoryBuffer.length() > 0);
    }

    public long getCreateTime() {
        return createTime;
    }

    public void delete() {
        if (file.exists()) {
            FileUtils.deleteQuietly(file);
        }

        if (memoryBuffer != null) {
            memoryBuffer.setLength(0);
            memoryBuffer = null;
        }
    }

    public File getFile() {
        return file;
    }

    public String getPath() {
        return path;
    }

}
