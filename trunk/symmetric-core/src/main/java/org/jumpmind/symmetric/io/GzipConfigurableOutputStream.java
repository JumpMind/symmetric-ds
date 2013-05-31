package org.jumpmind.symmetric.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * 
 */
public class GzipConfigurableOutputStream extends DeflaterOutputStream {

    /**
     * CRC-32 of uncompressed data.
     */
    protected CRC32 crc = new CRC32();

    /*
     * GZIP header magic number.
     */
    private final static int GZIP_MAGIC = 0x8b1f;

    /**
     * Creates a new output stream with the specified buffer size.
     * 
     * @param out
     *            the output stream
     * @param size
     *            the output buffer size
     * @exception IOException
     *                If an I/O error has occurred.
     * @exception IllegalArgumentException
     *                if size is <= 0
     */
    public GzipConfigurableOutputStream(OutputStream out, int size,
            int compression) throws IOException {
        super(out, new Deflater(compression, true), size);
        writeHeader();
        crc.reset();
    }

    /**
     * Creates a new output stream with a default buffer size.
     * 
     * @param out
     *            the output stream
     * @exception IOException
     *                If an I/O error has occurred.
     */
    public GzipConfigurableOutputStream(OutputStream out, int compression)
            throws IOException {
        this(out, 512, compression);
    }

    /**
     * Writes array of bytes to the compressed output stream. This method will
     * block until all the bytes are written.
     * 
     * @param buf
     *            the data to be written
     * @param off
     *            the start offset of the data
     * @param len
     *            the length of the data
     * @exception IOException
     *                If an I/O error has occurred.
     */
    public synchronized void write(byte[] buf, int off, int len)
            throws IOException {
        super.write(buf, off, len);
        crc.update(buf, off, len);
    }

    /**
     * Finishes writing compressed data to the output stream without closing the
     * underlying stream. Use this method when applying multiple filters in
     * succession to the same output stream.
     * 
     * @exception IOException
     *                if an I/O error has occurred
     */
    public void finish() throws IOException {
        if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
                deflate();
            }
            writeTrailer();
        }
    }

    /**
     * Writes remaining compressed data to the output stream and closes the
     * underlying stream.
     * 
     * @exception IOException
     *                if an I/O error has occurred
     */
    public void close() throws IOException {
        finish();
        out.close();
    }

    /*
     * Writes GZIP member header.
     */
    private void writeHeader() throws IOException {
        writeShort(GZIP_MAGIC); // Magic number
        out.write(Deflater.DEFLATED); // Compression method (CM)
        out.write(0); // Flags (FLG)
        writeInt(0); // Modification time (MTIME)
        out.write(0); // Extra flags (XFL)
        out.write(0); // Operating system (OS)
    }

    /*
     * Writes GZIP member trailer.
     */
    private void writeTrailer() throws IOException {
        writeInt((int) crc.getValue()); // CRC-32 of uncompressed data
        writeInt(def.getTotalIn()); // Number of uncompressed bytes
    }

    /*
     * Writes integer in Intel byte order.
     */
    private void writeInt(int i) throws IOException {
        writeShort(i & 0xffff);
        writeShort((i >> 16) & 0xffff);
    }

    /*
     * Writes short integer in Intel byte order.
     */
    private void writeShort(int s) throws IOException {
        out.write(s & 0xff);
        out.write((s >> 8) & 0xff);
    }
}