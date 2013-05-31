package org.jumpmind.symmetric.config;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * An {@link IExtensionPoint} that allows a client to listen in on the trigger creation
 * process.
 *
 * 
 */
public interface ITriggerCreationListener extends IExtensionPoint {

    public void triggerCreated(Trigger trigger, TriggerHistory history);

    public void triggerFailed(Trigger trigger, Exception ex);

    public void triggerInactivated(Trigger trigger, TriggerHistory oldHistory);

    public void tableDoesNotExist(Trigger trigger);

}