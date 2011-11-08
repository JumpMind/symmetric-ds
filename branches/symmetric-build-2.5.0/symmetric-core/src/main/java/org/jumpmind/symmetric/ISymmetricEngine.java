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
package org.jumpmind.symmetric;

import java.util.Properties;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.DeploymentType;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.OutgoingPurgeJob;
import org.jumpmind.symmetric.job.PullJob;
import org.jumpmind.symmetric.job.PushJob;
import org.jumpmind.symmetric.job.RouterJob;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.IUpgradeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transform.ITransformService;
import org.springframework.context.ApplicationContext;

public interface ISymmetricEngine {

    public void stop();
    
    public void destroy();

    /**
     * @return the URL that represents this engine
     */
    public String getSyncUrl();

    /**
     * Get a list of configured properties for Symmetric. Read-only.
     */
    public Properties getProperties();

    /**
     * @return The lower case representation of the engine name as setup in the
     *         symmetric.properties file. We always use a lower case
     *         representation because there are times the engine name is used in
     *         triggers at which point you can lose the original case
     *         representation.
     */
    public String getEngineName();

    /**
     * Will setup the SymmetricDS tables, if not already setup and if the engine
     * is configured to do so.
     */
    public void setup();

    /**
     * Must be called to start SymmetricDS.
     * @return true if successfully started
     */
    public boolean start();
    
    public boolean start(boolean startJobs);

    /**
     * Queue up an initial load or a reload to a node.
     */
    public String reloadNode(String nodeId);

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName, String sql);

    /**
     * Will perform a push the same way the {@link PushJob} would have.
     * 
     * @see IPushService#pushData()
     * @return {@link RemoteNodeStatuses} 
     */
    public RemoteNodeStatuses push();

    /**
     * Call this to resync triggers
     * 
     * @see ITriggerRouterService#syncTriggers()
     */
    public void syncTriggers();

    /**
     * Get the current status of this node.
     * 
     * @return {@link NodeStatus}
     */
    public NodeStatus getNodeStatus();

    /**
     * Will perform a pull the same way the {@link PullJob} would have.
     * 
     * @see IPullService#pullData()
     * @return {@link RemoteNodeStatuses} 
     */
    public RemoteNodeStatuses pull();
    
    /**
     * Route captured data the same way the {@link RouterJob} would have.
     */
    public void route();

    /**
     * This can be called to do a purge. It may be called only if the
     * {@link OutgoingPurgeJob} has not been enabled.
     * 
     * @see IPurgeService#purgeOutgoing()
     */
    public void purge();

    /**
     * Will check to see if this instance of SymmetricDS is configured with the
     * required properties for a node to operate.
     */
    public boolean isConfigured();

    /**
     * Push a copy of the node onto the push queue so the SymmetricDS node
     * 'checks' in with it's root node.
     * @param force When force is true the heart beat will always be inserted.  If it is false, 
     * the heart beat will only be inserted if the period between heart beats has expired.
     */
    public void heartbeat(boolean force);

    /**
     * Open up registration for node to attach.
     * 
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String groupId, String externalId);

    public void reOpenRegistration(String nodeId);

    /**
     * Check to see if this node has been registered.
     * 
     * @return true if the node is registered
     */
    public boolean isRegistered();

    /**
     * Check to see if this node has been started.
     * 
     * @return true if the node is started
     */
    public boolean isStarted();

    /**
     * Check to see if this node is starting.
     * 
     * @return true if the node is starting
     */

    public boolean isStarting();
    
    
    /**
     * Attempt to configure the database objects that support SymmetricDS.  If they are 
     * out of date this method will attempt to alter the tables to bring them up to date.
     * @param force forces this action to be run regardless of the parameter settings
     */
    public void setupDatabase(boolean force);

    /**
     * Expose access to the Spring context. This is for advanced use only.
     * 
     * @return the Spring application context that SymmetricDS runs in
     */
    public ApplicationContext getApplicationContext();

    public IConfigurationService getConfigurationService();

    public IParameterService getParameterService();

    public INodeService getNodeService();

    public IRegistrationService getRegistrationService();

    public IUpgradeService getUpgradeService();

    public IClusterService getClusterService();

    public IPurgeService getPurgeService();

    public IDataService getDataService();

    public IDbDialect getDbDialect();

    public IJobManager getJobManager();
    
    public IOutgoingBatchService getOutgoingBatchService();
    
    public IAcknowledgeService getAcknowledgeService();
    
    public IBandwidthService getBandwidthService();
    
    public IDataExtractorService getDataExtractorService();
    
    public IDataLoaderService getDataLoaderService();
    
    public IIncomingBatchService getIncomingBatchService();   
    
    public IPullService getPullService();
    
    public IPushService getPushService();
    
    public IRouterService getRouterService();
    
    public ISecurityService getSecurityService();
    
    public IStatisticService getStatisticService();
    
    public ITriggerRouterService getTriggerRouterService();
    
    public IStatisticManager getStatisticManager();
    
    public DataSource getDataSource();
    
    public DeploymentType getDeploymentType();
    
    public ITransformService getTransformService();
    
    public String getTablePrefix();

}