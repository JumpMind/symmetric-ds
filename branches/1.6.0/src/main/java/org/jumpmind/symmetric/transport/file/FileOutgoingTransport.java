package org.jumpmind.symmetric.transport.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * An outgoing transport that writes to the file system
 */
public class FileOutgoingTransport implements IOutgoingTransport {

    File file;

    BufferedWriter out;

    public FileOutgoingTransport(File file) throws IOException {
        this.file = file;
    }

    public FileOutgoingTransport() throws IOException {
        this.file = AppUtils.createTempFile("extract");
    }

    public BufferedWriter open() throws IOException {
        out = new BufferedWriter(new FileWriter(file));
        return out;
    }

    public boolean isOpen() {
        return out != null;
    }

    public void close() throws IOException {
        out.close();
        out = null;
    }
    
    public File getFile() {
        return file;
    }


}
