package org.jumpmind.symmetric.service;

public interface IBandwidthService {

    public double getDownloadKbpsFor(String url, long sampleSize, long maxTestDuration);
    
}
