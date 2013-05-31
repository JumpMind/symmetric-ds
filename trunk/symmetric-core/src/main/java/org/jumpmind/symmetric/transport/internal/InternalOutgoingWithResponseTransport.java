
package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.TransportUtils;

public class InternalOutgoingWithResponseTransport implements IOutgoingWithResponseTransport {

    BufferedWriter writer = null;

    BufferedReader reader = null;
    
    OutputStream os = null;

    boolean open = true;

    InternalOutgoingWithResponseTransport(OutputStream os, InputStream respIs) throws IOException {
        this.os = os;
        this.writer = TransportUtils.toWriter(os);
        this.reader = TransportUtils.toReader(respIs);
    }
    
    public OutputStream openStream() {
        return os;
    }

    public BufferedReader readResponse() throws IOException {
        IOUtils.closeQuietly(writer);
        return reader;
    }

    public void close() {
        IOUtils.closeQuietly(os);
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(reader);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter openWriter() {
        return writer;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return configurationService.getSuspendIgnoreChannelLists();
    }
}