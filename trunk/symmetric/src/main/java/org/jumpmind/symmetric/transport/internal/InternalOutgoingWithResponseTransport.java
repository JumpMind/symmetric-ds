package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;

public class InternalOutgoingWithResponseTransport implements
        IOutgoingWithResponseTransport {

    BufferedWriter writer = null;

    BufferedReader reader = null;

    boolean open = true;

    InternalOutgoingWithResponseTransport(OutputStream pushOs,
            InputStream respIs) {
        writer = new BufferedWriter(new OutputStreamWriter(pushOs));
        reader = new BufferedReader(new InputStreamReader(respIs));
    }

    public BufferedReader readResponse() throws IOException {
        IOUtils.closeQuietly(writer);
        return reader;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(reader);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() throws IOException {
        return writer;
    }

}