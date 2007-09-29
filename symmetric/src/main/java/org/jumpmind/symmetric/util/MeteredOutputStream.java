package org.jumpmind.symmetric.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An OutputStream that easily allows the rate of output to be limited.
 * 
 * @author awilcox
 * 
 */
public class MeteredOutputStream extends OutputStream {

    public static final long KB = 1024;

    public static final long MB = 1048576;

    private final long rate;

    private final long pauseTime;

    private long currentTime = 0;

    private long bytesOutput = 0;

    private OutputStream out;

    public MeteredOutputStream(OutputStream out, long ratePerSecond) {
        this.out = out;
        rate = ratePerSecond;
        pauseTime = 10;
    }

    public MeteredOutputStream(OutputStream out, long ratePerSecond, long pauseTime) {
        this.out = out;
        rate = ratePerSecond;
        this.pauseTime = pauseTime;
    }

    @Override
    public void write(int b) throws IOException {
        while (true) {
            long time = System.currentTimeMillis() / 1000;

            if (time != currentTime) {
                currentTime = time;
                bytesOutput = 0;
            }

            if (bytesOutput < rate) {
                out.write(b);
                bytesOutput++;
                return;
            }

            try {
                Thread.sleep(pauseTime);
            } catch (InterruptedException ie) {
                // eat;
            }
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }
}
