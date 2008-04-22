package org.jumpmind.symmetric.web;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

public class ThrottledResponseWrapper extends HttpServletResponseWrapper
{
    private ByteArrayOutputStream output;

    private long maxBps = 10240L;

    private long threshold = 8192L;

    private long checkPoint= 1024L;

    public ThrottledResponseWrapper(HttpServletResponse response)
    {
        super(response);
        output = new ByteArrayOutputStream();
    }

    public ServletOutputStream getOutputStream()
    {
        return new ThrottledServletOutputStream(output,maxBps,threshold,checkPoint);
    }

    public PrintWriter getWriter()
    {
        return new PrintWriter(getOutputStream(), true);
    }

    public ByteArrayOutputStream getOutput()
    {
        return output;
    }

    public void setOutput(ByteArrayOutputStream output)
    {
        this.output = output;
    }

    public long getMaxBps()
    {
        return maxBps;
    }

    public void setMaxBps(long maxBps)
    {
        this.maxBps = maxBps;
    }

    public long getThreshold()
    {
        return threshold;
    }

    public void setThreshold(long threshold)
    {
        this.threshold = threshold;
    }

    public long getCheckPoint()
    {
        return checkPoint;
    }

    public void setCheckPoint(long checkPoint)
    {
        this.checkPoint = checkPoint;
    }
}
