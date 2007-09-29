package org.jumpmind.symmetric.transport.metered;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.util.MeteredOutputStream;

public class MeteredOutputStreamOutgoingTransport implements IOutgoingTransport
{
    private BufferedWriter writer;
    private MeteredOutputStream stream;

    public MeteredOutputStreamOutgoingTransport(OutputStream stream, long rate) 
    {
        this.stream = new MeteredOutputStream(stream, rate);
    }
    
    public void close() throws IOException
    {
        IOUtils.closeQuietly(writer);
        writer = null;
    }

    public boolean isOpen()
    {
        return writer != null;
    }

    public BufferedWriter open() throws IOException
    {
        writer = new BufferedWriter(new OutputStreamWriter(stream)); 
        return writer; 
    }

}
