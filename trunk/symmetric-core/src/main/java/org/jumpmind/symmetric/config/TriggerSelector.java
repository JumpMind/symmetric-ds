package org.jumpmind.symmetric.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;

/**
 * Utility class to pair down a list of triggers from a list of TriggerRouters
 *
 * 
 */
public class TriggerSelector {

    private Collection<TriggerRouter> triggers;

    public TriggerSelector(Collection<TriggerRouter> triggers) {
        this.triggers = triggers;
    }

    public List<Trigger> select() {
        List<Trigger> filtered = new ArrayList<Trigger>(triggers.size());
        for (TriggerRouter trigger : triggers) {
            if (!filtered.contains(trigger)) {
                filtered.add(trigger.getTrigger());
            }
        }
        return filtered;
    }
}