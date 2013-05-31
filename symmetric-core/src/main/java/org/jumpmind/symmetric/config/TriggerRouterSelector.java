package org.jumpmind.symmetric.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jumpmind.symmetric.model.TriggerRouter;

/**
 * Utility class to pair down a list of triggers.
 */
public class TriggerRouterSelector {

    private String channelId;
    private String targetNodeGroupId;
    private Collection<TriggerRouter> triggers;

    public TriggerRouterSelector(Collection<TriggerRouter> triggers, String channelId, String targetNodeGroupId) {
        this.triggers = triggers;
        this.channelId = channelId;
        this.targetNodeGroupId = targetNodeGroupId;
    }

    public List<TriggerRouter> select() {
        List<TriggerRouter> filtered = new ArrayList<TriggerRouter>();
        for (TriggerRouter trigger : triggers) {
            if (trigger.getTrigger().getChannelId().equals(channelId)
                    && (targetNodeGroupId == null || trigger.getRouter().getNodeGroupLink().getTargetNodeGroupId().equals(targetNodeGroupId))) {
                filtered.add(trigger);
            }
        }
        return filtered;
    }
}