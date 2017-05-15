/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.web;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.springframework.context.ApplicationContext;

public class ServerSymmetricEngine extends ClientSymmetricEngine {

    protected List<IUriHandler> uriHandlers;
    
    protected SymmetricEngineHolder engineHolder;
        
    protected Map<String, Integer> errorCountByNode = new HashMap<String, Integer>();

    public ServerSymmetricEngine(File propertiesFile) {
        super(propertiesFile);
    }
    
    public ServerSymmetricEngine(File propertiesFile, ApplicationContext springContext) {
        super(propertiesFile, springContext);
    }
    
    public ServerSymmetricEngine(File propertiesFile, ApplicationContext springContext, SymmetricEngineHolder engineHolder) {
        super(propertiesFile, springContext);
        this.engineHolder = engineHolder;
    }
    
    public SymmetricEngineHolder getEngineHolder() {
        return engineHolder;
    }
    
    @Override
    protected SecurityServiceType getSecurityServiceType() {
        return SecurityServiceType.SERVER;
    }

    @Override
    protected void init() {
        super.init();

        AuthenticationInterceptor authInterceptor = new AuthenticationInterceptor(nodeService);
        NodeConcurrencyInterceptor concurrencyInterceptor = new NodeConcurrencyInterceptor(
                concurrentConnectionManager, configurationService, statisticManager);

        this.uriHandlers = new ArrayList<IUriHandler>();
        this.uriHandlers.add(new AckUriHandler(parameterService, acknowledgeService,
                authInterceptor));
        this.uriHandlers.add(new PingUriHandler(parameterService));
        this.uriHandlers
                .add(new InfoUriHandler(parameterService, nodeService, configurationService));
        this.uriHandlers.add(new BandwidthSamplerUriHandler(parameterService));
        this.uriHandlers.add(new PullUriHandler(parameterService, nodeService,
                configurationService, dataExtractorService, registrationService, statisticManager, outgoingBatchService,
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new PushUriHandler(parameterService, dataLoaderService,
                statisticManager, nodeService, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new PushStatusUriHandler(parameterService, nodeCommunicationService, 
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new RegistrationUriHandler(parameterService, registrationService,
                concurrencyInterceptor));
        this.uriHandlers.add(new ConfigurationUriHandler(parameterService, dataExtractorService,
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new FileSyncPullUriHandler(this, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new FileSyncPushUriHandler(this, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new CopyNodeUriHandler(this, authInterceptor));
        if (parameterService.is(ParameterConstants.WEB_BATCH_URI_HANDLER_ENABLE)) {
            this.uriHandlers.add(new BatchUriHandler(parameterService, dataExtractorService));
        }
    }
    
    public synchronized int getErrorCountFor(String nodeId) {
        Integer errorCount = errorCountByNode.get(nodeId);
        if (errorCount == null) {
            errorCount = 1;
        }
        return errorCount;
    }
    
    public synchronized void incrementErrorCountForNode(String nodeId) {
        Integer errorCount = errorCountByNode.get(nodeId);
        if (errorCount == null) {
            errorCount = 1;
        }
        errorCountByNode.put(nodeId, errorCount+1);
    }
    
    public synchronized void resetErrorCountForNode(String nodeId) {
        errorCountByNode.put(nodeId, 0);
    }
    

    public List<IUriHandler> getUriHandlers() {
        return uriHandlers;
    }

}
