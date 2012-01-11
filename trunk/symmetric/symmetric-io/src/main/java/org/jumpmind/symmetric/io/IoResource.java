package org.jumpmind.symmetric.io;

import java.io.InputStream;

public interface IoResource {
    
    public InputStream open();
    
    public boolean exists();

}
