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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;

/**
 * Defines the trigger via which a table will be synchronized.
 */
public class TriggerRouter {

    static final Log logger = LogFactory.getLog(TriggerRouter.class);

    private static final long serialVersionUID = 8947288471097851573L;

    /**
     * This is the order in which the definitions will be processed.
     */
    private int initialLoadOrder;

    private Trigger trigger;

    private Router router;

    private Date createTime;

    private Date lastUpdateTime;

    private String lastUpdateBy;

    public TriggerRouter() {
        trigger = new Trigger();
        router = new Router();
        createTime = new Date();
        lastUpdateTime = new Date();
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

    /**
     * When dealing with columns, always use this method to order the columns so that the primary keys are first.
     */
    public Column[] orderColumnsForTable(Table table) {
        return trigger.orderColumnsForTable(table);
    }

    public int getInitialLoadOrder() {
        return initialLoadOrder;
    }

    public void setInitialLoadOrder(int order) {
        this.initialLoadOrder = order;
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    public Router getRouter() {
        return router;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public boolean isRouted(DataEventType event) {
        switch (event) {
        case INSERT:
            return router.isSyncOnInsert();
        case DELETE:
            return router.isSyncOnDelete();
        case UPDATE:
            return router.isSyncOnUpdate();
        default:
            return true;
        }
    }

    public String getTargetSchema(String defaultSchema) {
        if (router != null && !StringUtils.isBlank(router.getTargetSchemaName())) {
            return router.getTargetSchemaName();
        }
        if (trigger != null && !StringUtils.isBlank(trigger.getSourceSchemaName())) {
            return trigger.getSourceSchemaName();
        } else {
            return defaultSchema;
        }
    }

    public String getTargetCatalog(String defaultCatalog) {
        if (router != null && !StringUtils.isBlank(router.getTargetCatalogName())) {
            return router.getTargetCatalogName();
        }
        if (trigger != null && !StringUtils.isBlank(trigger.getSourceCatalogName())) {
            return trigger.getSourceCatalogName();
        } else {
            return defaultCatalog;
        }
    }

    public String getTargetTable() {
        if (router != null && !StringUtils.isBlank(router.getTargetTableName())) {
            return router.getTargetTableName();
        }
        if (trigger != null && !StringUtils.isBlank(trigger.getSourceTableName())) {
            return trigger.getSourceTableName();
        } else {
            return null;
        }
    }

    public String getQualifiedTargetTableName() {
        String catalog = getTargetCatalog(null);
        String schema = getTargetSchema(null);
        String tableName = getTargetTable();
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return tableName;
    }
}
