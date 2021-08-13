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
package org.jumpmind.symmetric.job;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushHeartbeatListener implements IHeartbeatListener, IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    private ISymmetricEngine engine;

    public PushHeartbeatListener(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public void heartbeat(Node me) {
        IParameterService parameterService = engine.getParameterService();
        if (parameterService.is(ParameterConstants.HEARTBEAT_ENABLED)) {
            ISymmetricDialect symmetricDialect = engine.getSymmetricDialect();
            boolean updateWithBatchStatus = parameterService.is(ParameterConstants.HEARTBEAT_UPDATE_NODE_WITH_BATCH_STATUS, false);
            int outgoingErrorCount = -1;
            int outgoingUnsentCount = -1;
            if (updateWithBatchStatus) {
                outgoingUnsentCount = engine.getOutgoingBatchService().countOutgoingBatchesUnsent();
                outgoingErrorCount = engine.getOutgoingBatchService().countOutgoingBatchesInError();
            }
            if (!parameterService.getExternalId().equals(me.getExternalId())
                    || !parameterService.getNodeGroupId().equals(me.getNodeGroupId())
                    || (parameterService.getSyncUrl() != null && !parameterService.getSyncUrl().equalsIgnoreCase(me.getSyncUrl()))
                    || !parameterService.getString(ParameterConstants.SCHEMA_VERSION, "").equals(me.getSchemaVersion())
                    || (engine.getDeploymentType() != null && !engine.getDeploymentType().equals(me.getDeploymentType()))
                    || !Version.version().equals(me.getSymmetricVersion())
                    || (engine.getParameterService().isRegistrationServer() && !Version.version().equals(me.getConfigVersion()))
                    || !symmetricDialect.getName().equals(me.getDatabaseType())
                    || !symmetricDialect.getVersion().equals(me.getDatabaseVersion())
                    || me.getBatchInErrorCount() != outgoingErrorCount
                    || me.getBatchToSendCount() != outgoingUnsentCount) {
                log.info("Some attribute(s) of node changed.  Recording changes");
                me.setDeploymentType(engine.getDeploymentType());
                me.setDeploymentSubType(engine.getDeploymentSubType());
                me.setSymmetricVersion(Version.version());
                me.setDatabaseType(engine.getTargetDialect().getName());
                me.setDatabaseVersion(engine.getTargetDialect().getVersion());
                me.setDatabaseName(engine.getDatabasePlatform().getName());
                me.setBatchInErrorCount(outgoingErrorCount);
                me.setBatchToSendCount(outgoingUnsentCount);
                me.setSchemaVersion(parameterService.getString(ParameterConstants.SCHEMA_VERSION));
                if (engine.getParameterService().isRegistrationServer()) {
                    me.setConfigVersion(Version.version());
                }
                if (parameterService.is(ParameterConstants.AUTO_UPDATE_NODE_VALUES)) {
                    log.info("Updating my node configuration info according to the symmetric properties");
                    me.setExternalId(parameterService.getExternalId());
                    me.setNodeGroupId(parameterService.getNodeGroupId());
                    if (!StringUtils.isBlank(parameterService.getSyncUrl())) {
                        me.setSyncUrl(parameterService.getSyncUrl());
                    }
                }
                engine.getNodeService().save(me);
            }
            log.debug("Updating my node info");
            Set<Node> targetNodes = new HashSet<Node>();
            targetNodes.addAll(engine.getNodeService().findNodesWhoPullFromMe());
            targetNodes.addAll(engine.getNodeService().findNodesToPushTo());
            if (engine.getOutgoingBatchService().countOutgoingBatchesUnsentHeartbeat() < targetNodes.size() || targetNodes.size() == 0) {
                engine.getNodeService().updateNodeHostForCurrentNode();
            }
            log.debug("Done updating my node info");
            if (!engine.getNodeService().isRegistrationServer()) {
                if (!symmetricDialect.getPlatform().getDatabaseInfo().isTriggersSupported()) {
                    engine.getDataService().insertHeartbeatEvent(me, false);
                    Set<Node> children = engine.getNodeService().findNodesThatOriginatedFromNodeId(me.getNodeId());
                    for (Node node : children) {
                        engine.getDataService().insertHeartbeatEvent(node, false);
                    }
                }
            }
        }
    }

    public long getTimeBetweenHeartbeatsInSeconds() {
        return engine.getParameterService().getLong(
                ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
    }
}