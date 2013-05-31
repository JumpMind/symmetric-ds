package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * An adapter for the trigger listener interface so you need only implement the
 * methods you are interested in.
 *
 * 
 */
public class TriggerCreationAdapter implements ITriggerCreationListener {

    public void tableDoesNotExist(Trigger trigger) {
    }

    public void triggerCreated(Trigger trigger, TriggerHistory history) {
    }

    public void triggerFailed(Trigger trigger, Exception ex) {
    }

    public void triggerInactivated(Trigger trigger, TriggerHistory oldHistory) {
    }
    
}