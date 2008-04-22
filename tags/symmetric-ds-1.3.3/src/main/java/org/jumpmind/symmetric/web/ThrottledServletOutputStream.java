package org.jumpmind.symmetric.web;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletOutputStream;

import org.jumpmind.symmetric.util.MeteredOutputStream;

public class ThrottledServletOutputStream extends ServletOutputStream
{
    private MeteredOutputStream stream;

    public ThrottledServletOutputStream(OutputStream output,long maxBps)
    {
        stream = new MeteredOutputStream(output, maxBps);
    }
    
    public ThrottledServletOutputStream(OutputStream output,long maxBps,long threshold)
    {
        stream = new MeteredOutputStream(output, maxBps, threshold);
    }
    
    public ThrottledServletOutputStream(OutputStream output,long maxBps,long threshold, long checkPoint)
    {
        stream = new MeteredOutputStream(output, maxBps, threshold, checkPoint);
    }
    @Override
    public void write(int b) throws IOException
    {
        stream.write(b);

    }

    public void write(byte[] b) throws IOException
    {
        stream.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        stream.write(b, off, len);
    }

    @Override
    public void close() throws IOException
    {
        
        super.close();
        stream.close();
    }

    @Override
    public void flush() throws IOException
    {
      
        super.flush();
        stream.flush();
    }
    
    
}
