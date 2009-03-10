package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public interface ITriggerCreationListener extends IExtensionPoint {

    public void triggerCreated(Trigger trigger, TriggerHistory history);

    public void triggerFailed(Trigger trigger, Exception ex);
    
    public void triggerInactivated(Trigger trigger, TriggerHistory oldHistory);
    
    public void tableDoesNotExist(Trigger trigger);
    
}
