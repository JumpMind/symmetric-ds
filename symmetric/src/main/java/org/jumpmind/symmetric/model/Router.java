/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.model;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Defines the trigger via which a table will be synchronized.
 */
public class Router {

    static final Log logger = LogFactory.getLog(Router.class);

    private static final long serialVersionUID = 8947288471097851573L;

    private static int maxRouterId;
    
    private int routerId;

    private String targetTableName;

    private String sourceGroupId;

    private String targetGroupId;

    private String targetSchemaName;

    private String targetCatalogName;

    private String routerName = null;
    
    /**
     * Default to routing all data to all nodes.
     */
    private String routerExpression = null;
    
    private String initialLoadSelect = null;

    private Date createdOn;

    private Date lastModifiedTime;

    private String updatedBy;

    public Router() {
        routerId = maxRouterId++;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    public void setLastModifiedTime(Date lastModifiedOn) {
        this.lastModifiedTime = lastModifiedOn;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public boolean hasChangedSinceLastTriggerBuild(Date lastTriggerBuildTime) {
        return lastTriggerBuildTime == null || getLastModifiedTime() == null
                || lastTriggerBuildTime.before(getLastModifiedTime());
    }

    public String getSourceGroupId() {
        return sourceGroupId;
    }

    public void setSourceGroupId(String domainName) {
        this.sourceGroupId = domainName;
    }

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetDomainName) {
        this.targetGroupId = targetDomainName;
    }

    public int getRouterId() {
        return routerId;
    }

    public void setRouterId(int routerId) {
        this.routerId = routerId;
        if (routerId >= maxRouterId) {
            maxRouterId = routerId + 1;
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

    public void setRouterName(String routerName) {
        this.routerName = routerName;
    }
    
    public String getRouterName() {
        return routerName;
    }
    
    public void setInitialLoadSelect(String intialLoadSelect) {
        this.initialLoadSelect = intialLoadSelect;
    }
    
    public String getInitialLoadSelect() {
        return initialLoadSelect;
    }    
    
    public String getRouterExpression() {
        return routerExpression;
    }

    public void setRouterExpression(String routingExpression) {
        this.routerExpression = routingExpression;
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
        return routerId;
    }

}
