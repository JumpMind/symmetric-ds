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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class PostgreSqlTriggerTemplate extends AbstractTriggerTemplate {

    public PostgreSqlTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        //@formatter:off        
        geometryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast(ST_AsEWKT($(tableAlias).\"$(columnName)\") as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        geographyColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast(ST_AsEWKT($(tableAlias).\"$(columnName)\") as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        xmlColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        arrayColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast(cast($(tableAlias).\"$(columnName)\" as numeric) as varchar) || '\"' end" ;
        dateColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS') || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US') || '\"' end" ;
        timeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'HH24:MI:SS.US') || '\"' end" ;
        dateTimeWithTimeZoneColumnTemplate =
        		"case when $(tableAlias).\"$(columnName)\" is null then '' else                                                      " +
        		"   case                                                                                                             " +
        		"   when extract(timezone_hour from $(tableAlias).\"$(columnName)\") <= 0 and                                        " +
        		"        extract(timezone_minute from $(tableAlias).\"$(columnName)\") <= 0 then                                      " +
        		"     '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||'-'||                           " +
        		"     lpad(cast(abs(round(extract(timezone_hour from $(tableAlias).\"$(columnName)\"))) as varchar),2,'0')||':'||           " +
        		"     lpad(cast(abs(round(extract(timezone_minute from $(tableAlias).\"$(columnName)\"))) as varchar), 2, '0') || '\"'      " +
        		"   when extract(timezone_hour from $(tableAlias).\"$(columnName)\") = 0 and                                        " +
        		"        extract(timezone_minute from $(tableAlias).\"$(columnName)\") >= 0 then                                      " +
        		"     '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||'+'||                           " +
        		"     lpad(cast(round(extract(timezone_hour from $(tableAlias).\"$(columnName)\")) as varchar),2,'0')||':'||           " +
        		"     lpad(cast(round(extract(timezone_minute from $(tableAlias).\"$(columnName)\")) as varchar), 2, '0') || '\"'      " +
                "   else                                                                                                             " +
        		"     '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||'+'||                           " +
        		"     lpad(cast(round(extract(timezone_hour from $(tableAlias).\"$(columnName)\")) as varchar),2,'0')||':'||                " +
        		"     lpad(cast(round(extract(timezone_minute from $(tableAlias).\"$(columnName)\")) as varchar), 2, '0') || '\"'           " +
        		"   end                                                                                                              " +
        		"end                                                                                                                 ";
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || pg_catalog.encode($(tableAlias).\"$(columnName)\", 'base64') || '\"' end" ;
        wrappedBlobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(defaultSchema)$(prefixName)_largeobject($(tableAlias).\"$(columnName)\") || '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = stringColumnTemplate;
        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                \n" +
"                                begin                                                                                                                                                                  \n" +
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 \n" +
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"                                    (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                        \n" +
"                                    values(                                                                                                                                                            \n" +
"                                      '$(targetTableName)',                                                                                                                                            \n" +
"                                      'I',                                                                                                                                                             \n" +
"                                      $(triggerHistoryId),                                                                                                                                             \n" +
"                                      $(columns),                                                                                                                                                      \n" +
"                                      $(channelExpression),                                                                                                                                                \n" +
"                                      $(txIdExpression),                                                                                                                                               \n" +
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   \n" +
"                                      $(externalSelect),                                                                                                                                               \n" +
"                                      " + getCreateTimeExpression(symmetricDialect) + "                                                                                                                \n" +
"                                    );                                                                                                                                                                 \n" +
"                                  end if;                                                                                                                                                              \n" +
"                                  $(custom_on_insert_text)                                                                                                                                             \n" +
"                                  return null;                                                                                                                                                         \n" +
"                                end;                                                                                                                                                                   \n" +
"                                $function$ language plpgsql;                                                                                                                                           " );

        sqlTemplates.put("insertPostTriggerTemplate" ,
"create trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                                \n" +
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );

        sqlTemplates.put("updateTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                \n" +
"                                declare var_row_data text; \n" +        
"                                declare var_old_data text; \n" +
"                                begin\n" +
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 \n" +
"                                    var_row_data := $(columns); \n" +
"                                    var_old_data := $(oldColumns); \n" +
"                                    if $(dataHasChangedCondition) then \n" +
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"                                    (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                     \n" +
"                                    values(                                                                                                                                                            \n" +
"                                      '$(targetTableName)',                                                                                                                                            \n" +
"                                      'U',                                                                                                                                                             \n" +
"                                      $(triggerHistoryId),                                                                                                                                             \n" +
"                                      $(oldKeys),                                                                                                                                                      \n" +
"                                      var_row_data,                                                                                                                                                      \n" +
"                                      var_old_data,                                                                                                                                                   \n" +
"                                      $(channelExpression),                                                                                                                                                \n" +
"                                      $(txIdExpression),                                                                                                                                               \n" +
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   \n" +
"                                      $(externalSelect),                                                                                                                                               \n" +
"                                      " + getCreateTimeExpression(symmetricDialect) + "                                                                                                                \n" +
"                                    );                                                                                                                                                                 \n" +
"                                  end if;                                                                                                                                                              \n" +
"                                  end if;                                                                                                                                                              \n" +
"                                  $(custom_on_update_text)                                                                                                                                             \n" +
"                                  return null;                                                                                                                                                         \n" +
"                                end;                                                                                                                                                                   \n" +
"                                $function$ language plpgsql;                                                                                                                                           " );

        sqlTemplates.put("updatePostTriggerTemplate" ,
"create trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                                \n" +
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );

        sqlTemplates.put("deleteTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                \n" +
"                                begin                                                                                                                                                                  \n" +
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 \n" +
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"                                    (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                               \n" +
"                                    values(                                                                                                                                                            \n" +
"                                      '$(targetTableName)',                                                                                                                                            \n" +
"                                      'D',                                                                                                                                                             \n" +
"                                      $(triggerHistoryId),                                                                                                                                             \n" +
"                                      $(oldKeys),                                                                                                                                                      \n" +
"                                      $(oldColumns),                                                                                                                                                   \n" +
"                                      $(channelExpression),                                                                                                                                                \n" +
"                                      $(txIdExpression),                                                                                                                                               \n" +
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   \n" +
"                                      $(externalSelect),                                                                                                                                               \n" +
"                                      " + getCreateTimeExpression(symmetricDialect) + "                                                                                                                \n" +
"                                    );                                                                                                                                                                 \n" +
"                                  end if;                                                                                                                                                              \n" +
"                                  $(custom_on_delete_text)                                                                                                                                             \n" +
"                                  return null;                                                                                                                                                         \n" +
"                                end;                                                                                                                                                                   \n" +
"                                $function$ language plpgsql;                                                                                                                                           " );

        sqlTemplates.put("deletePostTriggerTemplate" ,
"create trigger $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                                \n" +
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

    @Override
    protected boolean requiresWrappedBlobTemplateForBlobType() {
        return true;
    }
    
    protected String getCreateTimeExpression(ISymmetricDialect symmetricDialect) {
        String timezone = symmetricDialect.getParameterService().getString(ParameterConstants.DATA_CREATE_TIME_TIMEZONE);
        if (StringUtils.isEmpty(timezone)) {
            return "CURRENT_TIMESTAMP";
        } else {
            return String.format("CURRENT_TIMESTAMP AT TIME ZONE '%s'", timezone);
        }    
    }

}