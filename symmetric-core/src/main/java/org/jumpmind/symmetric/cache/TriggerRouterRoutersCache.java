package org.jumpmind.symmetric.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerRouter;

public class TriggerRouterRoutersCache {
    public TriggerRouterRoutersCache(Map<String, List<TriggerRouter>> triggerRoutersByTriggerId,
            Map<String, Router> routersByRouterId) {
        this.triggerRoutersByTriggerId = triggerRoutersByTriggerId;
        this.routersByRouterId = routersByRouterId;
    }

    public Map<String, List<TriggerRouter>> triggerRoutersByTriggerId = new HashMap<String, List<TriggerRouter>>();
    public Map<String, Router> routersByRouterId = new HashMap<String, Router>();
}
