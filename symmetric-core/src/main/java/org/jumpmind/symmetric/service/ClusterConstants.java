package org.jumpmind.symmetric.service;

/**
 * Names for jobs as locked by the {@link IClusterService} 
 */
public class ClusterConstants {
    
    public static final String STAGE_MANAGEMENT = "STAGE_MANAGEMENT";
    public static final String ROUTE = "ROUTE";
    public static final String PUSH = "PUSH";
    public static final String PULL = "PULL";
    public static final String REFRESH_CACHE = "REFRESH_CACHE";
    public static final String PURGE_OUTGOING = "PURGE_OUTGOING";
    public static final String PURGE_INCOMING = "PURGE_INCOMING";
    public static final String PURGE_STATISTICS = "PURGE_STATISTICS";
    public static final String PURGE_DATA_GAPS = "PURGE_DATA_GAPS";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String SYNCTRIGGERS = "SYNCTRIGGERS";
    public static final String WATCHDOG = "WATCHDOG";
    public static final String STATISTICS = "STATISTICS";
    public static final String FILE_SYNC_TRACKER = "FILE_SYNC_TRACKER";
    public static final String FILE_SYNC_PULL = "FILE_SYNC_PULL";
    public static final String FILE_SYNC_PUSH = "FILE_SYNC_PUSH";
    
}