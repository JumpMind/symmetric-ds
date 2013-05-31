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
import java.util.List;

import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;

public class ServerSymmetricEngine extends ClientSymmetricEngine {

    protected List<IUriHandler> uriHandlers;

    public ServerSymmetricEngine(File propertiesFile) {
        super(propertiesFile);
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
                configurationService, dataExtractorService, registrationService, statisticManager,
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new PushUriHandler(parameterService, dataLoaderService,
                statisticManager, nodeService, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new RegistrationUriHandler(parameterService, registrationService,
                concurrencyInterceptor));
        this.uriHandlers.add(new FileSyncPullUriHandler(this, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new FileSyncPushUriHandler(this, concurrencyInterceptor, authInterceptor));        
        if (parameterService.is(ParameterConstants.WEB_BATCH_URI_HANDLER_ENABLE)) {
            this.uriHandlers.add(new BatchUriHandler(parameterService, dataExtractorService));
        }
    }

    public List<IUriHandler> getUriHandlers() {
        return uriHandlers;
    }

}
