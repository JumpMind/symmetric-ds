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
package org.jumpmind.symmetric.db.firebird;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class FirebirdTriggerTemplate extends AbstractTriggerTemplate {

    public FirebirdTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);

        // @formatter:off
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || REPLACE(REPLACE($(tableAlias).\"$(columnName)\", '\\', '\\\\'), '\"', '\\\"') || '\"' end";
        clobColumnTemplate = stringColumnTemplate;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(tableAlias).\"$(columnName)\" || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(tableAlias).\"$(columnName)\" || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || sym_hex($(tableAlias).\"$(columnName)\") || '\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after insert as                                                                                                                            \n" +
"   declare variable id bigint;                                                                                                                                            \n" +
"   begin                                                                                                                                                                  \n" +
"     if ($(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               \n" +
"     begin                                                                                                                                                                \n" +
"       id = gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1);                                                                                                   \n" +
"       insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"       (data_id, table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                               \n" +
"       values(                                                                                                                                                            \n" +
"         :id,                                                                                                                                                             \n" +
"         '$(targetTableName)',                                                                                                                                            \n" +
"         'I',                                                                                                                                                             \n" +
"         $(triggerHistoryId),                                                                                                                                             \n" +
"         $(columns),                                                                                                                                                      \n" +
"         '$(channelName)',                                                                                                                                                \n" +
"         $(txIdExpression),                                                                                                                                               \n" +
"         rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           \n" +
"         $(externalSelect),                                                                                                                                               \n" +
"         CURRENT_TIMESTAMP                                                                                                                                                \n" +
"       );                                                                                                                                                                 \n" +
"     end                                                                                                                                                                  \n" +
"     $(custom_on_insert_text)                                                                                                                                             \n" +
"   end                                                                                                                                                                    \n" );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after update as                                                                                                                            \n" +
"   declare variable id bigint;                                                                                                                                            \n" +
"   begin                                                                                                                                                                  \n" +
"     if ($(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               \n" +
"     begin                                                                                                                                                                \n" +
"       id = gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1);                                                                                                   \n" +
"       insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"       (data_id, table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)            \n" +
"       values(                                                                                                                                                            \n" +
"         :id,                                                                                                                                                             \n" +
"         '$(targetTableName)',                                                                                                                                            \n" +
"         'U',                                                                                                                                                             \n" +
"         $(triggerHistoryId),                                                                                                                                             \n" +
"         $(oldKeys),                                                                                                                                                      \n" +
"         $(columns),                                                                                                                                                      \n" +
"         $(oldColumns),                                                                                                                                                   \n" +
"         '$(channelName)',                                                                                                                                                \n" +
"         $(txIdExpression),                                                                                                                                               \n" +
"         rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           \n" +
"         $(externalSelect),                                                                                                                                               \n" +
"         CURRENT_TIMESTAMP                                                                                                                                                \n" +
"       );                                                                                                                                                                 \n" +
"     end                                                                                                                                                                  \n" +
"     $(custom_on_update_text)                                                                                                                                             \n" +
"   end                                                                                                                                                                    \n" );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger  $(triggerName) for $(schemaName)$(tableName) after delete as                                                                                                                           \n" +
"   declare variable id bigint;                                                                                                                                            \n" +
"   begin                                                                                                                                                                  \n" +
"     if ($(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               \n" +
"     begin                                                                                                                                                                \n" +
"       id = gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1);                                                                                                   \n" +
"       insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"       (data_id, table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                      \n" +
"       values(                                                                                                                                                            \n" +
"         :id,                                                                                                                                                             \n" +
"         '$(targetTableName)',                                                                                                                                            \n" +
"         'D',                                                                                                                                                             \n" +
"         $(triggerHistoryId),                                                                                                                                             \n" +
"         $(oldKeys),                                                                                                                                                      \n" +
"         $(oldColumns),                                                                                                                                                   \n" +
"         '$(channelName)',                                                                                                                                                \n" +
"         $(txIdExpression),                                                                                                                                               \n" +
"         rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           \n" +
"         $(externalSelect),                                                                                                                                               \n" +
"         CURRENT_TIMESTAMP                                                                                                                                                \n" +
"       );                                                                                                                                                                 \n" +
"     end                                                                                                                                                                  \n" +
"     $(custom_on_delete_text)                                                                                                                                             \n" +
"   end                                                                                                                                                                    \n" );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}