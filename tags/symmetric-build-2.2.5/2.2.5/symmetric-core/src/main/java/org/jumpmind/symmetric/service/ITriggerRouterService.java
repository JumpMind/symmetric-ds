/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IExtraConfigTables;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;

/**
 * Provides an API to configure {@link TriggerRouter}s, {@link Trigger}s and {@link Router}s.
 */
public interface ITriggerRouterService {

    /**
     * Return a list of triggers used when extraction configuration data during 
     * the registration process.
     * @param sourceGroupId group id of the node being registered with
     * @param targetGroupId group id of the node that is registering
     */
    public List<TriggerRouter> getTriggerRoutersForRegistration(String version, NodeGroupLink nodeGroupLink, String... tablesToExclude);
    
    public Map<String, List<TriggerRouter>> getTriggerRoutersByChannel(String configurationTypeId);

    public Map<String, List<TriggerRouter>> getTriggerRoutersForCurrentNode(boolean refreshCache);

    /**
     * Get router that is currently in use by a trigger router at the node that is hosting this call.
     * @param routerId The router_id to retrieve
     * @param refreshCache Whether to force the router to be re-retrieved from the database
     */
    public Router getActiveRouterByIdForCurrentNode(String routerId, boolean refreshCache);
    
    public Router getRouterById(String routerId);
    
    public List<Router> getRouters();
    
    /**
     * Get a list of routers for a specific node group link.
     */
    public List<Router> getRoutersByGroupLink(NodeGroupLink link);
    
    public boolean isRouterBeingUsed(String routerId);    
    
    public void deleteRouter(Router router);
    
    public void saveRouter(Router router);
    
    public List<TriggerRouter> getAllTriggerRoutersForCurrentNode(String sourceNodeGroupId);
    
    /**
     * Get a list of all the triggers that have been defined for the system.
     */
    public List<Trigger> getTriggers();
    
    public void saveTrigger(Trigger trigger);

    public void deleteTrigger(Trigger trigger);
    
    public void createTriggersOnChannelForTables(String channelId, Set<Table> tables, String lastUpdateBy);
    
    public boolean isTriggerBeingUsed(String triggerId);
    
    public boolean doesTriggerExist(String triggerId);
    
    public boolean doesTriggerExistForTable(String tableName);
    
    public List<TriggerRouter> getAllTriggerRoutersForReloadForCurrentNode(String sourceNodeGroupId, String targetNodeGroupId);

    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(NodeGroupLink link, String catalogName, String schemaName, String tableName, boolean refreshCache);
    
    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(String catalog, String schema, String tableName, boolean refreshCache); 

    public TriggerRouter findTriggerRouterById(String triggerId, String routerId);

    public void inactivateTriggerHistory(TriggerHistory history);

    public TriggerHistory getNewestTriggerHistoryForTrigger(String triggerId);

    public TriggerHistory getTriggerHistory(int historyId);
    
    public TriggerHistory findTriggerHistory(String sourceTableName);
    
    public Trigger getTriggerById(String triggerId);

    public void insert(TriggerHistory newAuditRecord);

    public Map<Long, TriggerHistory> getHistoryRecords();

    public void deleteTriggerRouter(TriggerRouter triggerRouter);
    
    public void saveTriggerRouter(TriggerRouter triggerRouter, boolean updateTriggerRouterTableOnly);
    
    public void saveTriggerRouter(TriggerRouter trigger);
        
    public void syncTriggers();

    public void syncTriggers(StringBuilder sqlBuffer, boolean gen_always);
    
    public void addTriggerCreationListeners(ITriggerCreationListener l);
    
    public void addExtraConfigTables(IExtraConfigTables extension);

    public Map<Trigger, Exception> getFailedTriggers();

}