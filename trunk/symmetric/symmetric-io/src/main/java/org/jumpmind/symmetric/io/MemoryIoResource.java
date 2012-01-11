package org.jumpmind.symmetric.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class MemoryIoResource implements IoResource {

    byte[] buffer;

    public MemoryIoResource(byte[] buffer) {
        this.buffer = buffer;
    }

    public InputStream open() {
        return new ByteArrayInputStream(buffer);
    }
    
    public boolean exists() {
        return buffer != null && buffer.length > 0;
    }

}
