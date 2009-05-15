/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.transport.BandwidthTestResults;

/**
 * @see IBandwidthService
 */
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
            bw.start();
            is = u.openStream();            
            int r;
            while (-1 != (r = is.read(buffer)) && bw.getElapsed() <= maxTestDuration) {
                bw.transmitted(r);
            }
            is.close();
            bw.stop();
            logger.info(String.format("%s was calculated to have a download bandwidth of %s kbps", syncUrl, bw.getKbps()));
            return bw;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

}
