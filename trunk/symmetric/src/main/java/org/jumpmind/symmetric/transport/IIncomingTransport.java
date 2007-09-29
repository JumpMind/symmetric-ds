package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.IOException;

public interface IIncomingTransport {

    public BufferedReader open() throws IOException;

    public void close() throws IOException;

    public boolean isOpen();
}
