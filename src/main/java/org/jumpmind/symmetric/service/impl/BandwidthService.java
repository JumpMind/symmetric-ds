package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.transport.BandwidthTestResults;

public class BandwidthService implements IBandwidthService {

    protected final Log logger = LogFactory.getLog(getClass());

    public double getDownloadKbpsFor(String syncUrl, long sampleSize, long maxTestDuration) {
        double downloadSpeed = -1;
        try {
            BandwidthTestResults bw = getDownloadResultsFor(syncUrl, sampleSize, maxTestDuration);
            downloadSpeed = (int) bw.getKbps();
        } catch (Exception e) {
            logger.error(e,e);
        }
        return downloadSpeed;

    }

    protected BandwidthTestResults getDownloadResultsFor(String syncUrl, long sampleSize, long maxTestDuration)
            throws IOException {
        byte[] buffer = new byte[1024];
        InputStream is = null;
        try {
            BandwidthTestResults bw = new BandwidthTestResults();
            URL u = new URL(String.format("%s/bandwidth?sampleSize=%s", syncUrl, sampleSize));
            is = u.openStream();
            bw.start();
            int r;
            while (-1 != (r = is.read(buffer)) && bw.getElapsed() <= maxTestDuration) {
                bw.transmitted(r);
            }
            is.close();
            bw.stop();
            return bw;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

}
