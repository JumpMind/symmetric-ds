/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * Provides an API to configure data synchronizations.
 */
public interface IConfigurationService {

    public List<NodeGroupLink> getGroupLinks();

    public List<NodeGroupLink> getGroupLinksFor(String sourceGroupId);

    public List<String> getRootConfigChannelTableNames();

    public void initTriggerRowsForConfigChannel();

    public void saveChannel(Channel channel);

    public void deleteChannel(Channel channel);

    public DataEventAction getDataEventActionsByGroupId(String sourceGroupId, String targetGroupId);

    public Map<String, List<Trigger>> getTriggersByChannelFor(String configurationTypeId);

    public void inactivateTriggerHistory(TriggerHistory history);

    public TriggerHistory getLatestHistoryRecordFor(int triggerId);

    public List<NodeChannel> getChannels();
    
    public void flushChannels();
    
    public List<NodeChannel> getChannelsFor(boolean failIfTableDoesNotExist);

    public List<Trigger> getActiveTriggersForSourceNodeGroup(String sourceNodeGroupId);

    public List<Trigger> getActiveTriggersForReload(String sourceNodeGroupId, String targetNodeGroupId);

    public List<Trigger> getInactiveTriggersForSourceNodeGroup(String sourceNodeGroupId);

    public TriggerHistory getHistoryRecordFor(int auditId);

    public Trigger getTriggerFor(String table, String sourceNodeGroupId);

    public Trigger getTriggerForTarget(String table, String sourceNodeGroupId, String targetDomainName, String channel);

    public Trigger getTriggerById(int triggerId);

    public void insert(TriggerHistory newAuditRecord);

    public Map<Long, TriggerHistory> getHistoryRecords();

    public void insert(Trigger trigger);

    public boolean isRegistrationServer();

}
