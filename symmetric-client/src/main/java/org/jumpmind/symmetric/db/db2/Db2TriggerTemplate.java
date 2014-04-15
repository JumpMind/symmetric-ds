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

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class Db2TriggerTemplate extends AbstractTriggerTemplate {

    public Db2TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"') || '\"' end" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || trim(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || rtrim(char(year(timestamp_iso($(tableAlias).\"$(columnName)\"))))||'-'||substr(digits(month(timestamp_iso($(tableAlias).\"$(columnName)\"))),9)||'-'||substr(digits(day(timestamp_iso($(tableAlias).\"$(columnName)\"))),9)||' '||substr(digits(hour(timestamp_iso($(tableAlias).\"$(columnName)\"))),9)||':'||substr(digits(minute(timestamp_iso($(tableAlias).\"$(columnName)\"))),9)||':'||substr(digits(second(timestamp_iso($(tableAlias).\"$(columnName)\"))),9)||'.'||rtrim(char(microsecond(timestamp_iso($(tableAlias).\"$(columnName)\")))) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar(32672)),'\\','\\\\'),'\"','\\\"') || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || hex(cast($(tableAlias).\"$(columnName)\" as varchar(16336) for bit data)) || '\"' end" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = null;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER INSERT ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING NEW AS NEW                                                                                                                                                 " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    IF $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " +
"                                            (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                " +
"                                        VALUES('$(targetTableName)', 'I', $(triggerHistoryId),                                                                                                         " +
"                                            $(columns),                                                                                                                                                " +
"                                            '$(channelName)', $(txIdExpression), $(sourceNodeExpression),                                                                                              " +
"                                            $(externalSelect),                                                                                                                                         " +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_insert_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );

        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    DECLARE var_row_data VARCHAR(16336);                                                                                                                               " +
"                                    DECLARE var_old_data VARCHAR(16336);                                                                                                                               " +
"                                    IF $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        SET var_row_data = $(columns);                                                                                                                                 " +
"                                        SET var_old_data = $(oldColumns);                                                                                                                              " +
"                                        IF $(dataHasChangedCondition) THEN                                                                                                                             " +
"                                            INSERT into $(defaultSchema)$(prefixName)_data                                                                                                             " +
"                                                (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)         " +
"                                            VALUES('$(targetTableName)', 'U', $(triggerHistoryId),                                                                                                     " +
"                                                $(oldKeys),                                                                                                                                            " +
"                                                var_row_data,                                                                                                                                          " +
"                                                var_old_data,                                                                                                                                          " +
"                                                '$(channelName)',                                                                                                                                      " +
"                                                $(txIdExpression),                                                                                                                                     " +
"                                                $(sourceNodeExpression),                                                                                                                               " +
"                                                $(externalSelect),                                                                                                                                     " +
"                                                CURRENT_TIMESTAMP);                                                                                                                                    " +
"                                        END IF;                                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_update_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );

        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER DELETE ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING OLD AS OLD                                                                                                                                                 " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    IF $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " +
"                                            (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                       " +
"                                        VALUES ('$(targetTableName)', 'D', $(triggerHistoryId),                                                                                                        " +
"                                            $(oldKeys),                                                                                                                                                " +
"                                            $(oldColumns),                                                                                                                                             " +
"                                            '$(channelName)',                                                                                                                                          " +
"                                            $(txIdExpression),                                                                                                                                         " +
"                                            $(sourceNodeExpression),                                                                                                                                   " +
"                                            $(externalSelect),                                                                                                                                         " +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_delete_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}