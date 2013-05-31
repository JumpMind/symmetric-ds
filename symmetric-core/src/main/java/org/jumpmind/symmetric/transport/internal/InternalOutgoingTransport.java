package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class InternalOutgoingTransport implements IOutgoingTransport {

    BufferedWriter writer = null;
    
    OutputStream os = null;

    ChannelMap map = null;

    boolean open = true;

    public InternalOutgoingTransport(OutputStream os, String encoding) throws UnsupportedEncodingException {
        this(os, new ChannelMap(), encoding);
    }

    public InternalOutgoingTransport(OutputStream os, ChannelMap map, String encoding) throws UnsupportedEncodingException {
        this.os = os;
        this.writer = new BufferedWriter(new OutputStreamWriter(os, encoding == null ? Charset.defaultCharset().name() : encoding));
        this.map = map;
    }

    public InternalOutgoingTransport(BufferedWriter writer) {
        this.writer = writer;
        this.map = new ChannelMap();
    }

    public void close() {
        IOUtils.closeQuietly(writer);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }
    
    public OutputStream openStream() {
        return os;
    }

    public BufferedWriter openWriter() {
        return writer;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return map;
    }

}