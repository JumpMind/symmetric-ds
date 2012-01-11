package org.jumpmind.symmetric.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.jumpmind.exception.IoException;

public class FileIoResource implements IoResource {

    File file;

    public FileIoResource(File file) {
        this.file = file;
    }

    public InputStream open() {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new IoException(e);
        }
    }
    
    public boolean exists() {
        return file.exists();
    }


}
