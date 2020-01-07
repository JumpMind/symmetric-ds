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
package org.jumpmind.symmetric.db;

import java.io.IOException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IAlterDatabaseInterceptor;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.MultiInstanceofPredicate;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseUpgradeListener implements IDatabaseUpgradeListener, ISymmetricEngineAware, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ISymmetricEngine engine;

    protected boolean isUpgradeFromPre38;
    protected boolean isUpgradeFrom38;
    
    @Override
    public String beforeUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database currentModel,
            Database desiredModel) throws IOException {
        StringBuilder sb = new StringBuilder();
        
        isUpgradeFromPre38 = isUpgradeFromPre38(tablePrefix, currentModel, desiredModel);

        if (isUpgradeFromPre38) {
            Table transformTable = currentModel.findTable(tablePrefix + "_" + TableConstants.SYM_TRANSFORM_TABLE);
            if (transformTable != null && transformTable.findColumn("update_action") != null) {
                engine.getSqlTemplate().update("update " + tablePrefix + "_" + TableConstants.SYM_TRANSFORM_TABLE +
                        " set update_action = 'UPD_ROW' where update_action is null");
            }

            String dataGapTableName = tablePrefix + "_" + TableConstants.SYM_DATA_GAP;
            if (currentModel.findTable(dataGapTableName) != null) { 
                engine.getSqlTemplate().update("delete from " + dataGapTableName);
            }
            
            String nodeCommunicationTable = tablePrefix + "_" + TableConstants.SYM_NODE_COMMUNICATION;
            if (currentModel.findTable(nodeCommunicationTable) != null) {
                engine.getSqlTemplate().update("delete from " + tablePrefix + "_" + TableConstants.SYM_NODE_COMMUNICATION);
            }
        }
        
        if (isUpgradeFromPre311(tablePrefix, currentModel, desiredModel)) {
            log.info("Checking data_event for upgrade");
            
            List<Row> rows = engine.getDatabasePlatform().getSqlTemplateDirty().query("select batch_id, data_id, max(router_id) router_id " + 
                    "from " + tablePrefix + "_data_event group by batch_id, data_id having count(*) > 1");
            
            log.info("Found {} rows in data_event with duplicates", rows.size());

            if (rows.size() > 0) {
                long ts = System.currentTimeMillis();
                int commitSize = engine.getParameterService().getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
                ISqlTransaction transaction = null;
                try {
                    transaction = engine.getSqlTemplate().startSqlTransaction();
                    transaction.setInBatchMode(true);
                    transaction.prepare("delete from " + tablePrefix + "_data_event where batch_id = ? and data_id = ? and router_id != ?");
                    int[] types = new int[] { engine.getSymmetricDialect().getSqlTypeForIds(), engine.getSymmetricDialect().getSqlTypeForIds(),
                            Types.VARCHAR };
                    int uncommittedCount = 0, totalRowCount = 0;
                    for (Row row : rows) {
                        uncommittedCount += transaction.addRow(row, new Object[] { row.getLong("batch_id"), row.getLong("data_id"),
                                row.getString("router_id") }, types);
                        totalRowCount++;
                        if (uncommittedCount >= commitSize) {
                            transaction.commit();
                            uncommittedCount = 0;
                        }
                        if (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE) {
                            log.info("Processed {} of {} rows so far", totalRowCount, rows.size());
                            ts = System.currentTimeMillis();
                        }
                    }
                    transaction.commit();
                } catch (Error ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } catch (RuntimeException ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } finally {
                    if (transaction != null) {
                        transaction.close();
                    }
                }
            }

            log.info("Done preparing data_event for upgrade");
        }

        if (engine.getDatabasePlatform().getName().equals(DatabaseNamesConstants.INFORMIX)) {
            Table triggerTable = desiredModel.findTable(tablePrefix + "_" + TableConstants.SYM_TRIGGER);
            if (triggerTable != null) {
                for (Column column : triggerTable.getColumns()) {
                    if (column.getMappedTypeCode() == Types.LONGVARCHAR) {
                        column.setJdbcTypeCode(Types.VARCHAR);
                        column.setMappedType("VARCHAR");
                        column.setMappedTypeCode(Types.VARCHAR);
                        column.setSize("255");
                    }
                }
            }
        }
        
        if (engine.getDatabasePlatform().getName().equals(DatabaseNamesConstants.MYSQL)) {
            String function = tablePrefix + "_transaction_id_post_5_7_6";
            String select = "select count(*) from information_schema.routines where routine_name='"
                    + function + "' and routine_schema in (select database())";

            if (engine.getDatabasePlatform().getSqlTemplate().queryForInt(select) > 0) {
                String drop = "drop function " + function;
                engine.getDatabasePlatform().getSqlTemplate().update(drop);
                log.info("Just uninstalled {}", function);
            }
        }
        
        // Leave this last in the sequence of steps to make sure to capture any DML changes done before this
        if (engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            // Drop triggers on sym tables
            List<IAlterDatabaseInterceptor> alterDatabaseInterceptors =
                    engine.getExtensionService().getExtensionPointList(IAlterDatabaseInterceptor.class);
            List<IModelChange> modelChanges = engine.getDatabasePlatform().getDdlBuilder().getDetectedChanges(currentModel,
                    desiredModel,
                    alterDatabaseInterceptors.toArray(new IAlterDatabaseInterceptor[alterDatabaseInterceptors.size()]));
            
            Predicate predicate = new MultiInstanceofPredicate(new Class[] {
                    RemovePrimaryKeyChange.class,
                    AddPrimaryKeyChange.class,
                    PrimaryKeyChange.class,
                    RemoveColumnChange.class,
                    AddColumnChange.class,
                    ColumnDataTypeChange.class,
                    ColumnSizeChange.class,
                    CopyColumnValueChange.class
            });
            @SuppressWarnings("unchecked")
            Collection<TableChange> modelChangesAffectingTriggers = CollectionUtils.select(modelChanges, predicate);
            Set<String> setOfTableNamesToDropTriggersFor = new HashSet<String>();
            for(TableChange change: modelChangesAffectingTriggers) {
                setOfTableNamesToDropTriggersFor.add(change.getChangedTable().getName());
            }
            engine.getTriggerRouterService().dropTriggers(setOfTableNamesToDropTriggersFor);
        }
        
        return sb.toString();
    }

    @Override
    public String afterUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database model) throws IOException {
        
        // Leave this first so triggers are put back in place before any DML is done against SymmetricDS tables
        // Reinstall triggers on sym tables
        engine.getTriggerRouterService().syncTriggers();
        
        StringBuilder sb = new StringBuilder();
        if (isUpgradeFromPre38) {
            engine.getSqlTemplate().update("update " + tablePrefix + "_" + TableConstants.SYM_SEQUENCE +
                    " set cache_size = 10 where sequence_name = ?", Constants.SEQUENCE_OUTGOING_BATCH);
            engine.getSqlTemplate().update("update  " + tablePrefix + "_" + TableConstants.SYM_CHANNEL +
                    " set max_batch_size = 10000 where reload_flag = 1 ");
        }

        engine.getPullService().pullConfigData(false);

        return sb.toString();
    }

    protected void checkForDroppedColumns(Database currentModel, Database desiredModel) {
        for (Table currentTable : currentModel.getTables()) {
            Table desiredTable = desiredModel.findTable(currentTable.getName());
            if (desiredTable != null) {
                for (Column currentColumn : currentTable.getColumns()) {
                    Column desiredColumn = desiredTable.findColumn(currentColumn.getName());
                    if (desiredColumn == null) {
                        dropTriggers(currentModel, currentTable.getName(), currentColumn.getName());
                        break;
                    }
                }
            }
        }
    }

    protected void dropTriggers(Database currentModel, String tableName, String columnName) {
        Table table = currentModel.findTable(tableName);
        if (table != null && table.findColumn(columnName) != null) {
            TriggerHistory hist = engine.getTriggerRouterService().findTriggerHistory(null, null, tableName);
            if (hist != null) {
                log.info("Dropping triggers on " + tableName + " because " + columnName + " needs dropped");
                engine.getTriggerRouterService().dropTriggers(hist);
            }
        }
    }
    
    protected boolean isUpgradeFromPre38(String tablePrefix, Database currentModel,
            Database desiredModel) {
        String monitorTableName = tablePrefix + "_" + TableConstants.SYM_MONITOR;
        String nodeTableName = tablePrefix + "_" + TableConstants.SYM_NODE;
        if (currentModel.findTable(nodeTableName) != null && 
                currentModel.findTable(monitorTableName) == null && desiredModel.findTable(monitorTableName) != null) {
            log.info("Detected upgrade from pre-3.8 version.");
            return true;
        } else {
            return false;
        }        
    }
    protected boolean isUpgradeFromPre311(String tablePrefix, Database currentModel, Database desiredModel) {
        Table eventTable = currentModel.findTable(tablePrefix + "_" + TableConstants.SYM_DATA_EVENT);
        if (eventTable != null && eventTable.findColumn("router_id") != null) {
            log.info("Detected upgrade from pre-3.11 version.");
            return true;
        } else {
            return false;
        }        
    }
    
    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

}
