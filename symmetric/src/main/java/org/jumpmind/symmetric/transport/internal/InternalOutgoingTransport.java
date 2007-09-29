package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class InternalOutgoingTransport implements IOutgoingTransport {

    BufferedWriter writer = null;

    boolean open = true;

    public InternalOutgoingTransport(OutputStream pushOs) {
        writer = new BufferedWriter(new OutputStreamWriter(pushOs));
    }

    public InternalOutgoingTransport(BufferedWriter writer) {
        this.writer = writer;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() throws IOException {
        return writer;
    }

}
