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
package org.jumpmind.symmetric.android;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ITypedPropertiesFactory;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.sqlite.SqliteSymmetricDialect;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.route.ChannelRouterContext;
import org.jumpmind.symmetric.route.DataGapRouteReader;
import org.jumpmind.symmetric.route.IDataToRouteReader;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.impl.NodeCommunicationService;
import org.jumpmind.symmetric.service.impl.RouterService;

import android.content.Context;
import android.database.sqlite.SQLiteOpenHelper;

public class AndroidSymmetricEngine extends AbstractSymmetricEngine {

    protected String registrationUrl;
    protected String externalId;
    protected String nodeGroupId;
    protected Properties properties;
    protected SQLiteOpenHelper databaseHelper;
    protected Context androidContext;

    public AndroidSymmetricEngine(String registrationUrl, String externalId, String nodeGroupId,
            Properties properties, SQLiteOpenHelper databaseHelper, Context androidContext) {
        super(true);
        this.deploymentType = "android";
        this.registrationUrl = registrationUrl;
        this.externalId = externalId;
        this.nodeGroupId = nodeGroupId;
        this.properties = properties;
        this.databaseHelper = databaseHelper;
        this.androidContext = androidContext;
        init();
    }
    
    @Override
    protected SecurityServiceType getSecurityServiceType() {
        return SecurityServiceType.CLIENT;
    }

    @Override
    protected ITypedPropertiesFactory createTypedPropertiesFactory() {
        return new AndroidTypedPropertiesFactory(registrationUrl, externalId, nodeGroupId,
                properties);
    }

    @Override
    protected IDatabasePlatform createDatabasePlatform(TypedProperties properties) {
        return new AndroidDatabasePlatform(databaseHelper, androidContext);
    }

    @Override
    protected IStagingManager createStagingManager() {
        return new IStagingManager() {

            public IStagedResource find(Object... path) {
                return null;
            }

            public IStagedResource create(Object... path) {
                return null;
            }

            public long clean() {
                return 0;
            }
            
            public long clean(long timeToLiveInMs) {
                return 0;
            }
        };
    }

    @Override
    protected ISymmetricDialect createSymmetricDialect() {
        return new SqliteSymmetricDialect(parameterService, platform);
    }

    @Override
    protected IJobManager createJobManager() {
        return new AndroidJobManager(this);
    }

    @Override
    protected IRouterService buildRouterService() {
        return new AndroidRouterService(this);
    }

    class AndroidRouterService extends RouterService {

        public AndroidRouterService(ISymmetricEngine engine) {
            super(engine);
        }

        @Override
        protected IDataToRouteReader startReading(ChannelRouterContext context) {
            IDataToRouteReader reader = new DataGapRouteReader(context, engine);
            // not going to read on a separate thread in android
            reader.run();
            return reader;
        }

    }

    @Override
    protected INodeCommunicationService buildNodeCommunicationService(IClusterService clusterService, INodeService nodeService,
            IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        return new AndroidNodeCommunicationService(clusterService, nodeService, parameterService, symmetricDialect);
    }

    class AndroidNodeCommunicationService extends NodeCommunicationService {

        public AndroidNodeCommunicationService(IClusterService clusterService, INodeService nodeService,
                IParameterService parameterService, ISymmetricDialect symmetricDialect) {
            super(clusterService, nodeService, parameterService, symmetricDialect);
        }

        @Override
        public boolean execute(NodeCommunication nodeCommunication, RemoteNodeStatuses statuses,
                INodeCommunicationExecutor executor) {
            final RemoteNodeStatus status = statuses.add(nodeCommunication.getNode());
            long ts = System.currentTimeMillis();
            boolean failed = false;
            try {
                executor.execute(nodeCommunication, status);
                failed = status.failed();
            } catch (Throwable ex) {
                failed = true;
                log.error(String.format("Failed to execute %s for node %s", nodeCommunication
                        .getCommunicationType().name(), nodeCommunication.getNodeId()), ex);
            } finally {
                long millis = System.currentTimeMillis() - ts;
                nodeCommunication.setLockTime(null);
                nodeCommunication.setLastLockMillis(millis);
                if (failed) {
                    nodeCommunication.setFailCount(nodeCommunication.getFailCount() + 1);
                    nodeCommunication.setTotalFailCount(nodeCommunication.getTotalFailCount() + 1);
                    nodeCommunication.setTotalFailMillis(nodeCommunication.getTotalFailMillis()
                            + millis);
                } else {
                    nodeCommunication.setSuccessCount(nodeCommunication.getSuccessCount() + 1);
                    nodeCommunication
                            .setTotalSuccessCount(nodeCommunication.getTotalSuccessCount() + 1);
                    nodeCommunication.setTotalSuccessMillis(nodeCommunication
                            .getTotalSuccessMillis() + millis);
                    nodeCommunication.setFailCount(0);
                }
                status.setComplete(true);
                save(nodeCommunication);
            }
            return !failed;
        }

        @Override
        public int getAvailableThreads(CommunicationType communicationType) {
            return 10;
        }

    }
    
    public File snapshot() {
        return null;
    }
    
    public List<File> listSnapshots() {
        return new ArrayList<File>(0);
    }

}
