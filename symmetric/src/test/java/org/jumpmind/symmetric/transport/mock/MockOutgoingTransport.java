package org.jumpmind.symmetric.transport.mock;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;

import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class MockOutgoingTransport implements IOutgoingTransport {

    private StringWriter writer = new StringWriter();
    private BufferedWriter bWriter;
    
    public MockOutgoingTransport() {
    }

    public void close() throws IOException {
        bWriter.flush();
    }

    public BufferedWriter open() throws IOException {
        bWriter = new BufferedWriter(writer);
        return bWriter;
    }

    public boolean isOpen() {
        return true;
    }

    public String toString() {
        try {
            bWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer.getBuffer().toString();
    }

}
