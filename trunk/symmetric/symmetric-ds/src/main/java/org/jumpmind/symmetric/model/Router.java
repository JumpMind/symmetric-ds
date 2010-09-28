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


package org.jumpmind.symmetric.model;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Defines the trigger via which a table will be synchronized.
 *
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class Router {

    static final Log logger = LogFactory.getLog(Router.class);

    private static final long serialVersionUID = 8947288471097851573L;

    private static int maxRouterId;

    private String routerId;

    private String sourceNodeGroupId;

    private String targetNodeGroupId;

    private String routerType = null;

    /**
     * Default to routing all data to all nodes.
     */
    private String routerExpression = null;

    private boolean syncOnUpdate = true;

    private boolean syncOnInsert = true;

    private boolean syncOnDelete = true;
    
    private String targetCatalogName;
    
    private String targetSchemaName;

    private String targetTableName;

    private Date createTime;

    private Date lastUpdateTime;

    private String lastUpdateBy;

    public Router() {
        routerId = Integer.toString(maxRouterId++);
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setCreateTime(Date createdOn) {
        this.createTime = createdOn;
    }

    public void setLastUpdateTime(Date lastModifiedOn) {
        this.lastUpdateTime = lastModifiedOn;
    }

    public void setLastUpdateBy(String updatedBy) {
        this.lastUpdateBy = updatedBy;
    }

    public boolean hasChangedSinceLastTriggerBuild(Date lastTriggerBuildTime) {
        return lastTriggerBuildTime == null || getLastUpdateTime() == null
                || lastTriggerBuildTime.before(getLastUpdateTime());
    }

    public String getSourceNodeGroupId() {
        return sourceNodeGroupId;
    }

    public void setSourceNodeGroupId(String domainName) {
        this.sourceNodeGroupId = domainName;
    }

    public String getTargetNodeGroupId() {
        return targetNodeGroupId;
    }

    public void setTargetNodeGroupId(String targetDomainName) {
        this.targetNodeGroupId = targetDomainName;
    }

    public String getRouterId() {
        return routerId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
        if (StringUtils.isNumeric(routerId)) {
            int id = Integer.parseInt(routerId);
            if (id >= maxRouterId) {
                maxRouterId = id + 1;
            }
        }
    }

    public String getTargetSchemaName() {
        return targetSchemaName;
    }

    public void setTargetSchemaName(String targetSchemaName) {
        this.targetSchemaName = targetSchemaName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetCatalogName() {
        return targetCatalogName;
    }

    public void setTargetCatalogName(String targetCatalogName) {
        this.targetCatalogName = targetCatalogName;
    }

    public void setRouterType(String routerName) {
        this.routerType = routerName;
    }

    public String getRouterType() {
        return routerType;
    }

    public String getRouterExpression() {
        return routerExpression;
    }

    public void setRouterExpression(String routingExpression) {
        this.routerExpression = routingExpression;
    }

    public void setSyncOnDelete(boolean syncOnDelete) {
        this.syncOnDelete = syncOnDelete;
    }
    
    public boolean isSyncOnDelete() {
        return syncOnDelete;
    }
    
    public void setSyncOnInsert(boolean syncOnInsert) {
        this.syncOnInsert = syncOnInsert;
    }
    
    public boolean isSyncOnInsert() {
        return syncOnInsert;
    }
    
    public void setSyncOnUpdate(boolean syncOnUpdate) {
        this.syncOnUpdate = syncOnUpdate;
    }
    
    public boolean isSyncOnUpdate() {
        return syncOnUpdate;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Router) {
            return routerId == ((Router) obj).routerId;

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return routerId != null ? routerId.hashCode() : super.hashCode();
    }

}