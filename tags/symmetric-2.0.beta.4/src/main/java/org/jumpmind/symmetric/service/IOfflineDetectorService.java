package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.io.IOfflineListener;

public interface IOfflineDetectorService {

    public void addOfflineListener(IOfflineListener listener);

    public boolean removeOfflineListener(IOfflineListener listener);

}
