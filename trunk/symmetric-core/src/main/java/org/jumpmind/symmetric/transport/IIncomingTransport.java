
package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

public interface IIncomingTransport {

    public BufferedReader openReader() throws IOException;
    
    public InputStream openStream() throws IOException;

    public void close();

    public boolean isOpen();
    
    public String getRedirectionUrl();
    
    public String getUrl();
}