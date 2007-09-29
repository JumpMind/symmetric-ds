package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;

public class InternalIncomingTransport implements IIncomingTransport {

    BufferedReader reader = null;

    public InternalIncomingTransport(InputStream pullIs) throws IOException {
        reader = TransportUtils.toReader(pullIs);
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
        reader = null;
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        return reader;
    }

}
