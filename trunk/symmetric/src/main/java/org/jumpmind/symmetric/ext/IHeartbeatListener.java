package org.jumpmind.symmetric.ext;

import java.util.Set;

import org.jumpmind.symmetric.job.HeartbeatJob;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that when registered will be called on a regular
 * basis by the {@link HeartbeatJob}. The frequency of the job is controlled by
 * a parameter.
 */
public interface IHeartbeatListener extends IExtensionPoint {

    public void heartbeat(Node me, Set<Node> children);

}
