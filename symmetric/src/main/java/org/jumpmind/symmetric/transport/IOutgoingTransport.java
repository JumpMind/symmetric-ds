package org.jumpmind.symmetric.transport;

import java.io.BufferedWriter;
import java.io.IOException;

public interface IOutgoingTransport {
    public BufferedWriter open() throws IOException;
    public void close() throws IOException;
    public boolean isOpen();
}
