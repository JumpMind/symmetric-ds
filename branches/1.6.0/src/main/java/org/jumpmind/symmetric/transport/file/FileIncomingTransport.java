package org.jumpmind.symmetric.transport.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.jumpmind.symmetric.transport.IIncomingTransport;

/**
 * An incoming stream that reads from a file.
 */
public class FileIncomingTransport implements IIncomingTransport {

    File file;

    BufferedReader reader;

    public FileIncomingTransport(File file) {
        this.file = file;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (file != null) {
            file.delete();
        }
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        reader = new BufferedReader(new FileReader(file));
        return reader;
    }

}
