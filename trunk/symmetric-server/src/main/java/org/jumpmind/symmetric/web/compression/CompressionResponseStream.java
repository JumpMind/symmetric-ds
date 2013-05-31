package org.jumpmind.symmetric.web.compression;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of <b>ServletOutputStream</b> that works with the
 * CompressionServletResponseWrapper implementation.
 * 
 * This package is derived from the Jakarta <a
 * href="http://jakarta.apache.org/tomcat">Tomcat</a> examples compression
 * filter and is distributed in SymmetricDS for convenience.
 * 
 * @author Amy Roh
 * @author Dmitri Valdin
 */
public class CompressionResponseStream extends ServletOutputStream {

    static final Logger logger = LoggerFactory.getLogger(CompressionResponseStream.class);

    /**
     * The underlying gzip output stream to which we should write data.
     */
    protected OutputStream gzipstream = null;

    /**
     * Has this stream been closed?
     */
    protected boolean closed = false;

    /**
     * The response with which this servlet output stream is associated.
     */
    protected HttpServletResponse response = null;

    /**
     * Construct a servlet output stream associated with the specified Response.
     * 
     * @param response
     *                The associated response
     */
    public CompressionResponseStream(HttpServletResponse response, final int compressionLevel, final int compressionStrategy) throws IOException {
        this.closed = false;
        this.response = response;
        response.addHeader("Content-Encoding", "gzip");
        gzipstream = new GZIPOutputStream(response.getOutputStream()) {
            {
                this.def.setLevel(compressionLevel);
                this.def.setStrategy(compressionStrategy);
            }
        };
    }

    /**
     * Close this output stream, causing any buffered data to be flushed and any
     * further output data to throw an IOException.
     */
    public void close() throws IOException {

        if (closed) {
            return;
        }

        if (gzipstream != null) {
            gzipstream.close();
            gzipstream = null;
        }

        closed = true;

    }

    /**
     * Flush any buffered data for this output stream, which also causes the
     * response to be committed.
     */
    public void flush() throws IOException {
        if (closed) {
            return;
        }

        if (gzipstream != null) {
            gzipstream.flush();
        }

    }

    /**
     * Write the specified byte to our output stream.
     * 
     * @param b
     *                The byte to be written
     * 
     * @exception IOException
     *                    if an input/output error occurs
     */
    public void write(int b) throws IOException {
        if (closed) {
            return;
        }

        write(new byte[] { (byte) b });

    }

    /**
     * Write <code>b.length</code> bytes from the specified byte array to our
     * output stream.
     * 
     * @param b
     *                The byte array to be written
     * 
     * @exception IOException
     *                    if an input/output error occurs
     */
    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * Write <code>len</code> bytes from the specified byte array, starting at
     * the specified offset, to our output stream.
     * 
     * @param b
     *                The byte array containing the bytes to be written
     * @param off
     *                Zero-relative starting offset of the bytes to be written
     * @param len
     *                The number of bytes to be written
     * 
     * @exception IOException
     *                    if an input/output error occurs
     */
    public void write(byte b[], int off, int len) throws IOException {
        if (closed || len == 0) {
            return;
        }

        gzipstream.write(b, off, len);
    }

    /**
     * Has this response stream been closed?
     */
    public boolean closed() {
        return this.closed;
    }

}