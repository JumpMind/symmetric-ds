package org.jumpmind.persist;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.jumpmind.exception.IoException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.stream.JsonWriter;

abstract public class AbstractJsonFileSystemPersister<T, K> implements IPersister<T, K> {

    static final String SUFFIX_WRITING = "writing";

    protected String directory;

    public AbstractJsonFileSystemPersister(String directory) {
        this.directory = directory;
        new File(directory).mkdirs();
    }

    protected abstract String buildFileNameFor(K key);   

    public T get(Class<T> clazz, K key) {
        T obj = null;
        String fileName = buildFileNameFor(key);
        File file = new File(directory, fileName);
        if (!file.exists()) {
            file = new File(directory, String.format("%s.%s", fileName, SUFFIX_WRITING));
        }

        if (file.exists()) {
            FileReader reader = null;
            try {
                reader = new FileReader(file);
                Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
                obj = gson.fromJson(reader, clazz);
            } catch (IOException ex) {
                throw new JsonIOException(ex);
            } finally {
                close(reader);
            }
        }
        return obj;
    }

    protected void close(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ex) {
        }
    }

    protected boolean delete(File file) {
        try {
            return file.delete();
        } catch (Exception ignored) {
            return false;
        }
    }

    public void save(T object, K key) {
        String fileName = buildFileNameFor(key);
        File writingFile = new File(directory, String.format("%s.%s", fileName, SUFFIX_WRITING));
        File finalFile = new File(directory, fileName);
        if (writingFile.exists()) {
            delete(writingFile);
        }
        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        JsonWriter writer = null;
        try {
            writer = new JsonWriter(new FileWriter(writingFile));
            writer.setSerializeNulls(true);
            writer.setIndent("  ");
            gson.toJson(object, object.getClass(), writer);
            if (finalFile.exists()) {
                delete(finalFile);
            }
            if (!writingFile.renameTo(finalFile)) {
                throw new IoException("Failed to rename %s to %s during save operation",
                        writingFile.getAbsolutePath(), finalFile.getAbsolutePath());
            }
        } catch (IOException ex) {
            throw new JsonIOException(ex);
        } finally {
            close(writer);
        }
    };
}
