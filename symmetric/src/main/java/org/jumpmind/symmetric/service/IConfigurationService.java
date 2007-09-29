package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * Provides an API to configure data synchronizations.
 */
public interface IConfigurationService {
    
    public List<NodeGroupLink> getGroupLinksFor(String sourceGroupId);
    
    public List<String> getRootConfigChannelTableNames();
    
    public List<String> getNodeConfigChannelTableNames();
    
    public void initTriggersForConfigTables(String configTable, String groupId, String targetGroupId);

    public void initConfigChannel();
    
    public Map<String, DataEventAction> getDataEventActionsByGroupId(String groupId);

    public Map<String, List<Trigger>> getTriggersByChannelFor(
            String configurationTypeId);

    public void inactivateTriggerHistory(TriggerHistory history);
    
    public TriggerHistory getLatestHistoryRecordFor(int triggerId);

    public List<Channel> getChannelsFor(String configurationId, boolean failIfTableDoesNotExist);
    
    public List<Trigger> getActiveTriggersForSourceNodeGroup(String sourceNodeGroupId); 
    
    public List<Trigger> getInactiveTriggersForSourceNodeGroup(String sourceNodeGroupId);
    
    public TriggerHistory getHistoryRecordFor(int auditId);
    
    public Trigger getTriggerFor (String table, String sourceNodeGroupId);
    
    public Trigger getTriggerForTarget(String table, String sourceNodeGroupId, String targetDomainName, String channel);

    public void insert(TriggerHistory newAuditRecord);

    public Map<Long, TriggerHistory> getHistoryRecords();

}
