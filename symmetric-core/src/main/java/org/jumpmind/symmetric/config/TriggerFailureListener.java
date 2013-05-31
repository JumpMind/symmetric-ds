package org.jumpmind.symmetric.config;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * A listener that was built specifically to 'listen' for failures.
 *
 * 
 */
public class TriggerFailureListener extends TriggerCreationAdapter {

    private Map<Trigger, Exception> failures;

    public TriggerFailureListener() {
        this.failures = new HashMap<Trigger, Exception>();
    }

    @Override
    public void triggerCreated(Trigger trigger, TriggerHistory history) {
        failures.remove(trigger);
    }

    @Override
    public void triggerInactivated(Trigger trigger, TriggerHistory oldHistory) {
        this.failures.remove(trigger);
    }

    @Override
    public void triggerFailed(Trigger trigger, Exception ex) {
        this.failures.put(trigger, ex);
    }

    public Map<Trigger, Exception> getFailures() {
        return failures;
    }

}