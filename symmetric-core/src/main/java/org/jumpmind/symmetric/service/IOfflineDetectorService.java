package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.io.IOfflineClientListener;

public interface IOfflineDetectorService {

    public void addOfflineListener(IOfflineClientListener listener);

    public boolean removeOfflineListener(IOfflineClientListener listener);

}