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
package org.jumpmind.symmetric.db.postgresql;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class PostgreSqlTriggerTemplate extends AbstractTriggerTemplate {

    public PostgreSqlTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        //@formatter:off
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        xmlColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        arrayColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end" ;
        dateColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS') || '\"' end" ;        
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US') || '\"' end" ;
        dateTimeWithTimeZoneColumnTemplate = 
        		"case when $(tableAlias).\"$(columnName)\" is null then '' else                                                      " +
        		"   case                                                                                                             " +
        		"   when extract(timezone_hour from $(tableAlias).\"$(columnName)\") < 0 then                                        " +
        		"     '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||'-'||                           " +
        		"     lpad(cast(abs(extract(timezone_hour from $(tableAlias).\"$(columnName)\")) as varchar),2,'0')||':'||           " +
        		"     lpad(cast(abs(extract(timezone_minute from $(tableAlias).\"$(columnName)\")) as varchar), 2, '0') || '\"'           " +
                "   else                                                                                                             " +
        		"     '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||'+'||                           " +
        		"     lpad(cast(extract(timezone_hour from $(tableAlias).\"$(columnName)\") as varchar),2,'0')||':'||                " +
        		"     lpad(cast(extract(timezone_minute from $(tableAlias).\"$(columnName)\") as varchar), 2, '0') || '\"'           " +
        		"   end                                                                                                              " +
        		"end                                                                                                                 ";
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || pg_catalog.encode($(tableAlias).\"$(columnName)\", 'base64') || '\"' end" ;
        wrappedBlobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(defaultSchema)$(prefixName)_largeobject($(tableAlias).\"$(columnName)\") || '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                        " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("insertPostTriggerTemplate" ,
"create trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                     " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'U',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("updatePostTriggerTemplate" ,
"create trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                               " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("deletePostTriggerTemplate" ,
"create trigger $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }
    
    @Override
    protected boolean requiresWrappedBlobTemplateForBlobType() {
        return true;
    }

}