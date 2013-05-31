package org.jumpmind.symmetric.service;


/**
 * A client service that determines bandwidth availability.
 * @see BandwidthSamplerServlet
 *
 * 
 */
public interface IBandwidthService {

    public double getDownloadKbpsFor(String url, long sampleSize, long maxTestDuration);
    
}