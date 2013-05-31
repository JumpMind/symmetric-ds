package org.jumpmind.symmetric.service;

import java.util.Calendar;

/**
 * This service provides an API to kick off purge processes with or 
 * without specific dates.
 * <p/>
 * This service will never purge data that has not been delivered to 
 * a target node that is still enabled.
 */
public interface IPurgeService {
    
    public long purgeOutgoing(boolean force);
    
    public long purgeIncoming(boolean force);
    
    public long purgeDataGaps(boolean force);    
    
    public long purgeDataGaps(Calendar retentionCutoff, boolean force);
    
    public long purgeOutgoing(Calendar retentionCutoff, boolean force);
    
    public long purgeIncoming(Calendar retentionCutoff, boolean force);

    public void purgeAllIncomingEventsForNode(String nodeId);
    
    public void purgeStats(boolean force);
    
}