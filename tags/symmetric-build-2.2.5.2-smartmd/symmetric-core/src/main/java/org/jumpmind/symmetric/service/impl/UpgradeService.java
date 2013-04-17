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


package org.jumpmind.symmetric.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IUpgradeService;
import org.jumpmind.symmetric.upgrade.IUpgradeTask;

/**
 * 
 */
public class UpgradeService extends AbstractService implements IUpgradeService {

    private INodeService nodeService;

    private Map<String, List<IUpgradeTask>> upgradeTaskMap;

    public boolean isUpgradeNecessary() {
        boolean isUpgradeNecessary = false;
        String symmetricVersion = nodeService.findSymmetricVersion();
        if (!StringUtils.isBlank(symmetricVersion) && !symmetricVersion.equals("development")) {
            if (Version.isOlderVersion(symmetricVersion)) {
                String nodeId = nodeService.findIdentityNodeId();
                if (nodeId != null) {
                    int[] fromVersion = Version.parseVersion(symmetricVersion);
                    isUpgradeNecessary = doUpgradeTasksExist(nodeId, fromVersion);
                    if (!isUpgradeNecessary && !parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE)) {
                        log.warn("UpgradeWarning", ParameterConstants.AUTO_CONFIGURE_DATABASE);
                    }
                }                
            }
        }
        return isUpgradeNecessary;
    }
    
    public boolean isUpgradePossible() {
        String symmetricVersion = nodeService.findSymmetricVersion();        
        if (!StringUtils.isBlank(symmetricVersion) && !symmetricVersion.equals("development")) {
            return (Version.parseVersion(symmetricVersion)[0] > 1);
        } else {
            return true;
        }
        
    }

    public void upgrade() {
        String symmetricVersion = nodeService.findSymmetricVersion();
        String nodeId = nodeService.findIdentityNodeId();
        if (symmetricVersion != null && nodeId != null) {
            int[] fromVersion = Version.parseVersion(symmetricVersion);
            if (Version.isOlderVersion(symmetricVersion)) {
                runUpgrade(nodeId, fromVersion);
                Node node = nodeService.findIdentity();
                node.setSymmetricVersion(Version.version());
                nodeService.updateNode(node);
            }
        } else {
            log.warn("NodeUpgradeFailed");
        }
    }
    
    protected boolean doUpgradeTasksExist(String nodeId, int[] fromVersion) {
        String majorMinorVersion = fromVersion[0] + "." + fromVersion[1];
        List<IUpgradeTask> upgradeTaskList = upgradeTaskMap.get(majorMinorVersion);
        return upgradeTaskList != null && upgradeTaskList.size() > 0;
    }

    private void runUpgrade(String nodeId, int[] fromVersion) {
        String majorMinorVersion = fromVersion[0] + "." + fromVersion[1];
        List<IUpgradeTask> upgradeTaskList = upgradeTaskMap.get(majorMinorVersion);
        log.warn("NodeUpgradeStarting", majorMinorVersion, Version.version());
        boolean isRegistrationServer = StringUtils.isEmpty(parameterService.getRegistrationUrl());
        if (upgradeTaskList != null) {
            for (IUpgradeTask upgradeTask : upgradeTaskList) {
                if ((isRegistrationServer && upgradeTask.isUpgradeRegistrationServer())
                        || (!isRegistrationServer && upgradeTask.isUpgradeNonRegistrationServer())) {
                    upgradeTask.upgrade(nodeId, parameterService, fromVersion);
                }
            }
        }
        log.warn("NodeUpgradeCompleted");
    }

    public void setUpgradeTaskMap(Map<String, List<IUpgradeTask>> upgradeTaskMap) {
        this.upgradeTaskMap = upgradeTaskMap;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}