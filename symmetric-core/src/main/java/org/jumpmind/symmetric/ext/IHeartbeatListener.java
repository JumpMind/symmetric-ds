package org.jumpmind.symmetric.ext;


import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that, when registered, will be called on a regular
 * basis by the {@link HeartbeatJob}.
 */
public interface IHeartbeatListener extends IExtensionPoint {

    /**
     * Called periodically when the heartbeat job is enabled.
     * 
     * @param me
     *            A representation of this instance. It's statistics will be
     *            updated prior to calling this method.
     */
    public void heartbeat(Node me);

    /**
     * @return The number of seconds between heartbeat notifications.
     */
    public long getTimeBetweenHeartbeatsInSeconds();

}