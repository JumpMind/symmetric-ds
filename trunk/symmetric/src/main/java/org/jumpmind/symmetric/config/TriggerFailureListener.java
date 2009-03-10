package org.jumpmind.symmetric.config;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.model.Trigger;

public class TriggerFailureListener extends TriggerCreationAdapter {

    Map<Trigger, Exception> failures;

    public TriggerFailureListener() {
        this.failures = new HashMap<Trigger, Exception>();
    }

    @Override
    public void triggerFailed(Trigger trigger, Exception ex) {
        this.failures.put(trigger, ex);
    }

    public Map<Trigger, Exception> getFailures() {
        return failures;
    }

    public void setFailures(Map<Trigger, Exception> failures) {
        this.failures = failures;
    }
}
