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
package org.jumpmind.symmetric.db.db2;

import org.jumpmind.symmetric.db.ISymmetricDialect;

public class Db2zOsTriggerTemplate extends Db2TriggerTemplate {

    public Db2zOsTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             \n" +
"                                AFTER INSERT ON $(schemaName)$(tableName)                                                                                                                              \n" +
"                                REFERENCING NEW AS NEW                                                                                                                                                 \n" +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               \n" +
"                                WHEN ($(syncOnInsertCondition) and $(syncOnIncomingBatchCondition))                                                                                                    \n" +
"                                BEGIN ATOMIC                                                                                                                                                           \n" +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 \n" +
"                                            (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                \n" +
"                                        VALUES('$(targetTableName)', 'I', $(triggerHistoryId),                                                                                                         \n" +
"                                            $(columns),                                                                                                                                                \n" +
"                                            $(channelExpression), $(txIdExpression), $(sourceNodeExpression),                                                                                          \n" +
"                                            $(externalSelect),                                                                                                                                         \n" +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        \n" +
"                                    $(custom_on_insert_text)                                                                                                                                           \n" +
"                                END                                                                                                                                                                    \n" );
        
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             \n"+
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              \n"+
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      \n"+
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               \n"+
"                                WHEN ($(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition))                                                                                                    \n"+
"                                BEGIN ATOMIC                                                                                                                                                           \n"+
"                                            INSERT into $(defaultSchema)$(prefixName)_data                                                                                                             \n"+
"                                                (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)         \n"+
"                                            VALUES('$(targetTableName)', 'U', $(triggerHistoryId),                                                                                                     \n"+
"                                                $(oldKeys),                                                                                                                                            \n"+
"                                                $(columns),                                                                                                                                            \n"+
"                                                $(oldColumns),                                                                                                                                         \n"+
"                                                $(channelExpression),                                                                                                                                  \n"+
"                                                $(txIdExpression),                                                                                                                                     \n"+
"                                                $(sourceNodeExpression),                                                                                                                               \n"+
"                                                $(externalSelect),                                                                                                                                     \n"+
"                                                CURRENT_TIMESTAMP);                                                                                                                                    \n"+
"                                    $(custom_on_update_text)                                                                                                                                           \n"+
"                                END                                                                                                                                                                    " );
        
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             \n" +
"                                AFTER DELETE ON $(schemaName)$(tableName)                                                                                                                              \n" +
"                                REFERENCING OLD AS OLD                                                                                                                                                 \n" +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               \n" +
"                                WHEN ($(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition))                                                                                                    \n" +
"                                BEGIN ATOMIC                                                                                                                                                           \n" +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 \n" +
"                                            (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                       \n" +
"                                        VALUES ('$(targetTableName)', 'D', $(triggerHistoryId),                                                                                                        \n" +
"                                            $(oldKeys),                                                                                                                                                \n" +
"                                            $(oldColumns),                                                                                                                                             \n" +
"                                            $(channelExpression),                                                                                                                                      \n" +
"                                            $(txIdExpression),                                                                                                                                         \n" +
"                                            $(sourceNodeExpression),                                                                                                                                   \n" +
"                                            $(externalSelect),                                                                                                                                         \n" +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        \n" +
"                                    $(custom_on_delete_text)                                                                                                                                           \n" +
"                                END                                                                                                                                                                    \n" );
        
    }

}