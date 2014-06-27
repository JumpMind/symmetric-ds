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

package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a node who has registered for sync updates. 
 */
public class Node implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private static final Logger log = LoggerFactory.getLogger(Node.class);

    private int MAX_VERSION_SIZE = 50;

    private String nodeId;

    private String nodeGroupId;

    private String externalId;

    private String syncUrl;

    /**
     * Record the version of the schema. This is recorded and managed by the
     * sync software.
     */
    private String schemaVersion;

    /**
     * Record the type of database the node hosts.
     */
    private String databaseType;

    private String symmetricVersion = Version.version();

    /**
     * Get the version of the database the node hosts.
     */
    private String databaseVersion;

    private boolean syncEnabled = true;

    private String createdAtNodeId;
    
    private int batchToSendCount;
    
    private int batchInErrorCount;
    
    private String deploymentType;
    
    public Node() {
    }
    
    public Node(String nodeId, String nodeGroupId) {
        this.nodeId = nodeId;
        this.externalId = nodeId;
        this.nodeGroupId = nodeGroupId;
    }    
    
    public Node(Properties properties) {
        setNodeGroupId(properties.getProperty(ParameterConstants.NODE_GROUP_ID));
        setExternalId(properties.getProperty(ParameterConstants.EXTERNAL_ID));
        setSyncUrl(properties.getProperty(ParameterConstants.SYNC_URL));
        setSchemaVersion(properties.getProperty(ParameterConstants.SCHEMA_VERSION));
    }

    public Node(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        setNodeGroupId(parameterService.getNodeGroupId());
        setExternalId(parameterService.getExternalId());
        setDatabaseType(symmetricDialect.getName());
        setDatabaseVersion(symmetricDialect.getVersion());
        setSyncUrl(parameterService.getSyncUrl());
        setSchemaVersion(parameterService.getString(ParameterConstants.SCHEMA_VERSION));
    }

    public Node(String nodeId, String syncURL, String version) {
        this.nodeId = nodeId;
        this.syncUrl = syncURL;
        this.schemaVersion = version;
    }

    public boolean equals(Object n) {
        return n != null && n instanceof Node && nodeId != null && nodeId.equals(((Node) n).getNodeId());
    }
    
    @Override
    public int hashCode() {
        return nodeId != null ? nodeId.hashCode() : super.hashCode();
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getSyncUrl() {
        return syncUrl;
    }

    public void setSyncUrl(String syncURL) {
        this.syncUrl = syncURL;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String version) {
        // abbreviate because we do not control the version
        this.schemaVersion = StringUtils.abbreviate(version, MAX_VERSION_SIZE);
    }

    public boolean isSyncEnabled() {
        return syncEnabled;
    }

    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    public void setDatabaseVersion(String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String domainId) {
        this.externalId = domainId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String domainName) {
        this.nodeGroupId = domainName;
    }

    public String getSymmetricVersion() {
        return symmetricVersion;
    }

    public void setSymmetricVersion(String symmetricVersion) {
        this.symmetricVersion = symmetricVersion;
    }

    public String toString() {
        return nodeGroupId + ":" + externalId + ":" + (nodeId == null ? "?" : nodeId);
    }

    public String getCreatedAtNodeId() {
        return createdAtNodeId;
    }

    public void setCreatedAtNodeId(String createdByNodeId) {
        this.createdAtNodeId = createdByNodeId;
    }

    public void setBatchInErrorCount(int batchesInErrorCount) {
        this.batchInErrorCount = batchesInErrorCount;
    }
    
    public int getBatchInErrorCount() {
        return batchInErrorCount;
    }
    
    public void setBatchToSendCount(int batchesToSendCount) {
        this.batchToSendCount = batchesToSendCount;
    }
    
    public int getBatchToSendCount() {
        return batchToSendCount;
    }
    
    public void setDeploymentType(String deploymentType) {
        this.deploymentType = deploymentType;
    }
    
    public String getDeploymentType() {
        return deploymentType;
    }
    
    public boolean requires13Compatiblity() {
        if (symmetricVersion != null) {
            if (symmetricVersion.equals("development")) {
                return false;
            }
            try {
                int[] currentVersion = Version.parseVersion(symmetricVersion);
                return currentVersion != null && currentVersion.length > 0 && currentVersion[0] <= 1;
            } catch (Exception ex) {
                log.warn(
                        "Could not parse the version {} for node {}.  Setting backwards compatibility mode to true",
                        symmetricVersion, nodeId);
                return true;
            }
        }
        return false;
    }
    
    public boolean isVersionGreaterThanOrEqualTo(int... targetVersion) {
        if (symmetricVersion != null) {
            if (symmetricVersion.equals("development")) {
                return true;
            }
            int[] currentVersion = Version.parseVersion(symmetricVersion);
            for (int i = 0; i < currentVersion.length; i++) {
                int j = currentVersion[i];
                if (targetVersion.length > i) {
                    if (j > targetVersion[i]) {
                        return true;
                    } else if (j < targetVersion[i]) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    
}