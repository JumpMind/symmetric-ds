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
package org.jumpmind.symmetric.db.mssql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2016SymmetricDialect extends MsSql2008SymmetricDialect {
    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";
    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";
    static final String SESSION_CONTEXT_FUNCTION_INSTALLED = "select count(case when object_definition(object_id('$(functionName)')) like '%SESSION_CONTEXT%' then 1 else null end)";
    static final String triggersDisabledFunctionSql = "create function dbo.$(functionName)() returns smallint   " +
            "\n  begin        " +
            "\n    declare @disabled varchar(50);      " +
            "\n    set @disabled = CONVERT(varchar(50), SESSION_CONTEXT(N'" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "'));    " +
            "\n    if @disabled is null      " +
            "\n      return 0;       " +
            "\n    else if @disabled = '2' " +
            "\n      return 2; " +
            "\n    return 1;         " +
            "\n  end                 ";
    static final String nodeDisabledFunctionSql = "create function dbo.$(functionName)() returns varchar(50)    " +
            "\n  begin                            " +
            "\n    declare @node varchar(50);     " +
            "\n    set @node = CONVERT(varchar(50), SESSION_CONTEXT(N'" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "'));   " +
            "\n    return @node;  " +
            "\n  end              ";
    protected Boolean supportsSessionContext = null;

    public MsSql2016SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSql2016TriggerTemplate(this);
    }

    @Override
    protected void createTriggersDisabledFunction(StringBuilder ddl) {
        if (supportsSessionContext()) {
            String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
            if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
                install(triggersDisabledFunctionSql, triggersDisabled, ddl);
            } else if (!installed(SESSION_CONTEXT_FUNCTION_INSTALLED, triggersDisabled)) {
                uninstall(SQL_DROP_FUNCTION, triggersDisabled);
                install(triggersDisabledFunctionSql, triggersDisabled, ddl);
            } else {
                log.info("Function " + triggersDisabled + " using SESSION_CONTEXT is already installed");
            }
        } else {
            super.createTriggersDisabledFunction(ddl);
        }
    }

    @Override
    protected void createNodeDisabledFunction(StringBuilder ddl) {
        if (supportsSessionContext()) {
            String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
            if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
                install(nodeDisabledFunctionSql, nodeDisabled, ddl);
            } else if (!installed(SESSION_CONTEXT_FUNCTION_INSTALLED, nodeDisabled)) {
                uninstall(SQL_DROP_FUNCTION, nodeDisabled);
                install(nodeDisabledFunctionSql, nodeDisabled, ddl);
            } else {
                log.info("Function " + nodeDisabled + " using SESSION_CONTEXT is already installed");
            }
        } else {
            super.createNodeDisabledFunction(ddl);
        }
    }

    @Override
    protected boolean supportsDisableTriggers() {
        if (supportsSessionContext()) {
            return true;
        } else {
            return super.supportsDisableTriggers();
        }
    }

    protected boolean supportsSessionContext() {
        if (supportsSessionContext == null) {
            try {
                getPlatform().getSqlTemplate().update("sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "', NULL");
                log.info("This database DOES support setting session context to disable triggers during a symmetricds data load");
                supportsSessionContext = true;
            } catch (Exception ex) {
                log.info("This database does NOT support setting session context to disable triggers during a symmetricds data load");
                supportsSessionContext = false;
            }
        }
        return supportsSessionContext == null ? false : supportsSessionContext;
    }

    @Override
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        if (supportsSessionContext()) {
            transaction.prepareAndExecute("sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "', '1';");
            if (nodeId == null) {
                nodeId = "";
            }
            transaction.prepareAndExecute("sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "', '" + nodeId + "';");
        } else {
            super.disableSyncTriggers(transaction, nodeId);
        }
    }

    @Override
    public void enableSyncTriggers(ISqlTransaction transaction) {
        if (supportsSessionContext()) {
            transaction.prepareAndExecute("sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "', NULL;");
            transaction.prepareAndExecute("sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "', NULL;");
        } else {
            super.enableSyncTriggers(transaction);
        }
    }
}
