package org.jumpmind.symmetric.transport.mock;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class MockOutgoingTransport implements IOutgoingTransport {

    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private StringWriter writer = new StringWriter();
    private BufferedWriter bWriter;

    public MockOutgoingTransport() {
    }

    public OutputStream openStream() {
        return bos;
    }
    
    public void close() {
        try {
            bWriter.flush();
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

    public BufferedWriter openWriter() {
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

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return new ChannelMap();
    }

}