package org.jumpmind.symmetric.core.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

abstract public class IoUtils {

    /**
     * The default buffer size to use.
     */
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    /**
     * Get the contents of an <code>InputStream</code> as a String using the
     * default character encoding of the platform.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedInputStream</code>.
     * 
     * @param input
     *            the <code>InputStream</code> to read from
     * @return the requested String
     * @throws NullPointerException
     *             if the input is null
     * @throws IoException
     *             if an I/O error occurs
     */
    public static String toString(InputStream input) {
        StringWriter sw = new StringWriter();
        copy(input, sw);
        return sw.toString();
    }

    /**
     * Unconditionally close an <code>InputStream</code>.
     * <p>
     * Equivalent to {@link InputStream#close()}, except any exceptions will be
     * ignored. This is typically used in finally blocks.
     * 
     * @param input
     *            the InputStream to close, may be null or already closed
     */
    public static void closeQuietly(InputStream input) {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }
    
    /**
     * Unconditionally close an <code>Reader</code>.
     * <p>
     * Equivalent to {@link Reader#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     *
     * @param input  the Reader to close, may be null or already closed
     */
    public static void closeQuietly(Reader input) {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

    /**
     * Get the contents of a <code>Reader</code> as a list of Strings, one entry
     * per line.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedReader</code>.
     * 
     * @param input
     *            the <code>Reader</code> to read from, not null
     * @return the list of Strings, never null
     * @throws NullPointerException
     *             if the input is null
     * @throws IoException
     *             if an I/O error occurs
     * @since Commons IO 1.1
     */
    public static List<String> readLines(Reader input) {
        try {
            BufferedReader reader = new BufferedReader(input);
            List<String> list = new ArrayList<String>();
            String line = reader.readLine();
            while (line != null) {
                list.add(line);
                line = reader.readLine();
            }
            return list;
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    /**
     * Copy bytes from an <code>InputStream</code> to an
     * <code>OutputStream</code>.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedInputStream</code>.
     * <p>
     * Large streams (over 2GB) will return a bytes copied value of
     * <code>-1</code> after the copy has completed since the correct number of
     * bytes cannot be returned as an int. For large streams use the
     * <code>copyLarge(InputStream, OutputStream)</code> method.
     * 
     * @param input
     *            the <code>InputStream</code> to read from
     * @param output
     *            the <code>OutputStream</code> to write to
     * @return the number of bytes copied
     * @throws NullPointerException
     *             if the input or output is null @ * if an I/O error occurs
     * @throws ArithmeticException
     *             if the byte count is too large
     * @since Commons IO 1.1
     */
    public static int copy(InputStream input, OutputStream output) {
        long count = copyLarge(input, output);
        if (count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }

    public static void copy(InputStream input, Writer output) {
        InputStreamReader in = new InputStreamReader(input);
        copy(in, output);
    }

    /**
     * Copy chars from a <code>Reader</code> to a <code>Writer</code>.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedReader</code>.
     * <p>
     * Large streams (over 2GB) will return a chars copied value of
     * <code>-1</code> after the copy has completed since the correct number of
     * chars cannot be returned as an int. For large streams use the
     * <code>copyLarge(Reader, Writer)</code> method.
     * 
     * @param input
     *            the <code>Reader</code> to read from
     * @param output
     *            the <code>Writer</code> to write to
     * @return the number of characters copied
     * @throws NullPointerException
     *             if the input or output is null @ * if an I/O error occurs
     * @throws ArithmeticException
     *             if the character count is too large
     * @since Commons IO 1.1
     */
    public static int copy(Reader input, Writer output) {
        long count = copyLarge(input, output);
        if (count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }

    /**
     * Copy chars from a large (over 2GB) <code>Reader</code> to a
     * <code>Writer</code>.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedReader</code>.
     * 
     * @param input
     *            the <code>Reader</code> to read from
     * @param output
     *            the <code>Writer</code> to write to
     * @return the number of characters copied
     * @throws NullPointerException
     *             if the input or output is null @ * if an I/O error occurs
     * @since Commons IO 1.3
     */
    public static long copyLarge(Reader input, Writer output) {
        try {
            char[] buffer = new char[DEFAULT_BUFFER_SIZE];
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
            return count;
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    /**
     * Copy bytes from a large (over 2GB) <code>InputStream</code> to an
     * <code>OutputStream</code>.
     * <p>
     * This method buffers the input internally, so there is no need to use a
     * <code>BufferedInputStream</code>.
     * 
     * @param input
     *            the <code>InputStream</code> to read from
     * @param output
     *            the <code>OutputStream</code> to write to
     * @return the number of bytes copied
     * @throws NullPointerException
     *             if the input or output is null @ * if an I/O error occurs
     * @since Commons IO 1.3
     */
    public static long copyLarge(InputStream input, OutputStream output) {
        try {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            long count = 0;
            int n = 0;
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                count += n;
            }
            return count;
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }
}
