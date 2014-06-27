package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.jumpmind.symmetric.io.GzipConfigurableOutputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Quick little test ot see what type of compression gets the best results
 */
public class CompressionExperiment {

    Log logger = org.apache.commons.logging.LogFactory.getLog(getClass());

    @Test
    public void testCompression() throws IOException {
        File csv = new File("/tmp/test.csv");
        if (csv.exists()) {
            Assert.assertTrue(csv.exists());
            printFileSize(csv);
            logger.info("Normal gzip");
            printFileSize(compress(csv, new IWrapCompression() {
                public OutputStream wrap(OutputStream os) throws IOException {
                    return new GZIPOutputStream(os);
                }
            }));
            logger.info("Buffered gzip");
            printFileSize(compress(csv, new IWrapCompression() {
                public OutputStream wrap(OutputStream os) throws IOException {
                    return new GZIPOutputStream(os, 18192);
                }
            }));
            logger.info("Max compress gzip orig");
            File max = compress(csv, new IWrapCompression() {
                public OutputStream wrap(OutputStream os) throws IOException {
                    return new GZIPOutputStream(os, 18192) {
                        {
                            def.setLevel(Deflater.BEST_COMPRESSION);
                            def.setStrategy(Deflater.FILTERED);
                        }
                    };
                }
            });
            printFileSize(max);
            uncompress(max);
            logger.info("Max compress gzip");
            max = compress(csv, new IWrapCompression() {
                public OutputStream wrap(OutputStream os) throws IOException {
                    return new GzipConfigurableOutputStream(os, 18192, Deflater.BEST_COMPRESSION);
                }
            });
            printFileSize(max);
            uncompress(max);
        }

    }

    protected void printFileSize(File file) {
        logger.info(file.getName() + " size is " + file.length() / 10000 + "kb");
    }

    protected void uncompress(File file) throws IOException {
        GZIPInputStream is = new GZIPInputStream(new FileInputStream(file));
        IOUtils.readLines(is);
    }

    protected File compress(File orig, IWrapCompression wrapper) throws IOException {
        File compressed = File.createTempFile("test.", ".gz");
        FileOutputStream fos = new FileOutputStream(compressed);
        OutputStream wos = wrapper.wrap(fos);
        FileInputStream fis = new FileInputStream(orig);
        IOUtils.copy(fis, wos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(wos);
        return compressed;
    }

    interface IWrapCompression {
        public OutputStream wrap(OutputStream os) throws IOException;
    }
}
