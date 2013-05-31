package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.model.Grouplet;
import org.jumpmind.symmetric.model.GroupletLink;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.TriggerRouterGrouplet;

public interface IGroupletService {
    
    public boolean refreshFromDatabase();
    
    public void clearCache();
    
    public List<Grouplet> getGrouplets(boolean refreshCache);
    
    public void deleteGrouplet(Grouplet grouplet);
    
    public boolean isSourceEnabled(TriggerRouter triggerRouter);
    
    public Set<Node> getTargetEnabled(TriggerRouter triggerRouter, Set<Node> targetNodes);
    
    public boolean isTargetEnabled(TriggerRouter triggerRouter, Node targetNode);
    
    public void saveGrouplet(Grouplet grouplet);
    
    public void saveGroupletLink (Grouplet grouplet, GroupletLink link);
    
    public void deleteGroupletLink(Grouplet grouplet, GroupletLink link);
    
    public void saveTriggerRouterGrouplet(Grouplet grouplet, TriggerRouterGrouplet triggerRouterGrouplet);
    
    public void deleteTriggerRouterGrouplet(Grouplet grouplet, TriggerRouterGrouplet triggerRouterGrouplet);
    
    public void deleteTriggerRouterGroupletsFor(TriggerRouter triggerRouter);

}
