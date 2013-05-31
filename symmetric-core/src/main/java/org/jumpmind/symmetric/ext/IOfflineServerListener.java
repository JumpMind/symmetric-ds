package org.jumpmind.symmetric.ext;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that, when registered, will be called on a regular
 * basis by the {@link WatchdogJob}.  It is used to process nodes that are detected
 * to be offline.  An offline node has a heartbeat older than a 
 * configured amount of time.
 *
 * 
 */
public interface IOfflineServerListener extends IExtensionPoint {
    public void clientNodeOffline(Node node);
}