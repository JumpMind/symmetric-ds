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

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the trigger via which a table will be synchronized.
 */
public class TriggerRouter implements Serializable {

    private static final long serialVersionUID = 1L;

    static final Logger logger = LoggerFactory.getLogger(TriggerRouter.class);

    /**
     * This is the order in which the definitions will be processed.
     */
    private int initialLoadOrder;
    
    private String initialLoadSelect;

    private Trigger trigger;

    private Router router;

    private Date createTime;

    private Date lastUpdateTime;

    private String lastUpdateBy;
    
    private boolean pingBackEnabled = false;

    public TriggerRouter() {
        this(new Trigger(), new Router());
    }
    
    public TriggerRouter(Trigger trigger, Router router) {
        this.trigger = trigger;
        this.router = router;
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
    
    public void setInitialLoadSelect(String initialLoadSelect) {
        this.initialLoadSelect = initialLoadSelect;
    }
    
    public String getInitialLoadSelect() {
        return initialLoadSelect;
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
        } else {
            return defaultSchema;
        }
    }

    public String getTargetCatalog(String defaultCatalog) {
        if (router != null && !StringUtils.isBlank(router.getTargetCatalogName())) {
            return router.getTargetCatalogName();
        } else {
            return defaultCatalog;
        }
    }

    public String getTargetTable(TriggerHistory triggerHistory) {
        if (router != null && !StringUtils.isBlank(router.getTargetTableName())) {
            return router.getTargetTableName();
        }
        if (triggerHistory != null) {
            return triggerHistory.getSourceTableName();
        }
        if (trigger != null && !StringUtils.isBlank(trigger.getSourceTableName())) {
            return trigger.getSourceTableName();
        } else {
            return null;
        }
    }
    
    public String qualifiedSourceTableName() {
        return trigger.qualifiedSourceTableName();
    }
    
    public String qualifiedSourceTablePrefix() {
        return trigger.qualifiedSourceTablePrefix();
    }

    public String qualifiedTargetTableName(TriggerHistory triggerHistory) {
        String catalog = getTargetCatalog(null);
        String schema = getTargetSchema(null);
        String tableName = getTargetTable(triggerHistory);
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return tableName;
    }
    
    public void setPingBackEnabled(boolean pingBackEnabled) {
        this.pingBackEnabled = pingBackEnabled;
    }
    
    public boolean isPingBackEnabled() {
        return pingBackEnabled;
    }
    
    public boolean isSame(TriggerRouter triggerRouter) {
        return (this.trigger == null && triggerRouter.trigger == null)
                || (this.trigger != null && triggerRouter.trigger != null && this.trigger
                        .matches(triggerRouter.trigger))
                && (this.router == null && triggerRouter.router == null)
                || (this.router != null && triggerRouter.router != null && this.router
                        .equals(triggerRouter.router));
    }
    
    @Override
    public String toString() {
        return (trigger != null ? trigger.toString() : "") + ":"
                + (router != null ? router.toString() : "");
    }
}