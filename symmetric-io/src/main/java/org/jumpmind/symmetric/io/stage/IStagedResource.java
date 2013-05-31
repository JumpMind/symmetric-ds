package org.jumpmind.symmetric.io.stage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

public interface IStagedResource {

    public enum State {
        CREATE, READY, DONE;

        public String getExtensionName() {
            return name().toLowerCase();
        }

    };

    public BufferedReader getReader();

    public BufferedWriter getWriter();
    
    public OutputStream getOutputStream();

    public InputStream getInputStream();    
    
    public File getFile();
    
    public void close();

    public long getSize();

    public State getState();
    
    public String getPath();
    
    public void setState(State state);
    
    public long getCreateTime();
    
    public boolean isFileResource();
    
    public void delete();
    
    public boolean exists();

}
