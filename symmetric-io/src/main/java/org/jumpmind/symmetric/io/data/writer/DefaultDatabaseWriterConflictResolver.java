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
package org.jumpmind.symmetric.io.data.writer;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.exception.ParseException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver extends AbstractDatabaseWriterConflictResolver {

    protected static final Logger log = LoggerFactory.getLogger(DefaultDatabaseWriterConflictResolver.class);

    protected boolean isTimestampNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
    		DynamicDefaultDatabaseWriter databaseWriter = (DynamicDefaultDatabaseWriter)writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = databaseWriter.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = databaseWriter.getPlatform().createDmlStatement(DmlType.FROM, targetTable
                , writer.getWriterSettings().getTextColumnExpression());
        Column column = targetTable.getColumnWithName(columnName);
        
        if (column == null) {
            throw new RuntimeException(String.format("Could not find a timestamp column with a name of %s on the table %s.  "
                    + "Please check your conflict resolution configuration", columnName, targetTable.getQualifiedTableName()));
        }
        
        String sql = stmt.getColumnsSql(new Column[] { column });

        Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                CsvData.ROW_DATA);
        String loadingStr = newData.get(columnName);

        Date loadingTs = null;
        Date existingTs = null;
        if (column.isTimestampWithTimezone()) {
            // Get the existingTs with timezone
            String existingStr = databaseWriter.getTransaction().queryForObject(sql, String.class,
                    objectValues);
            // If you are in this situation because of an instance where the conflict exists
            // because the row doesn't exist, then existing simply needs to be null
            if (existingStr != null) {
	            int split = existingStr.lastIndexOf(" ");
	            existingTs = FormatUtils.parseDate(existingStr.substring(0, split).trim(),
	                    FormatUtils.TIMESTAMP_PATTERNS,
	                    TimeZone.getTimeZone(existingStr.substring(split).trim()));
            }
            // Get the loadingTs with timezone
            int split = loadingStr.lastIndexOf(" ");
            loadingTs = FormatUtils.parseDate(loadingStr.substring(0, split).trim(),
                    FormatUtils.TIMESTAMP_PATTERNS,
                    TimeZone.getTimeZone(loadingStr.substring(split).trim()));
        } else {
            // Get the existingTs
            existingTs = databaseWriter.getTransaction().queryForObject(sql, Timestamp.class,
                    objectValues);
            // Get the loadingTs
            Object[] values = platform.getObjectValues(writer.getBatch().getBinaryEncoding(),
                    new String[] { loadingStr }, new Column[] { column });
            if (values[0] instanceof Date) {
                loadingTs = (Date) values[0];
            } else if (values[0] instanceof String &&
                    column.getJdbcTypeName().equalsIgnoreCase(TypeMap.DATETIME2)) {
                // SQL Server DateTime2 type is treated as a string internally.
                loadingTs = databaseWriter.getPlatform().parseDate(Types.VARCHAR, (String)values[0], false);
            } else {
                throw new ParseException("Could not parse " + columnName + " with a value of "
                        + loadingStr + " for purposes of conflict detection");
            }
        }

        return existingTs == null || loadingTs.compareTo(existingTs) > 0;
    }

    protected boolean isVersionNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter)writer;
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = databaseWriter.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = databaseWriter.getPlatform().createDmlStatement(DmlType.FROM, targetTable
                , writer.getWriterSettings().getTextColumnExpression());
        String sql = stmt.getColumnsSql(new Column[] { targetTable.getColumnWithName(columnName) });
        Long existingVersion = null;
        
        try {            
            existingVersion = databaseWriter.getTransaction().queryForObject(sql, Long.class, objectValues);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to execute conflict resolution SQL: \"" + 
                    sql  + "\" values: " + Arrays.toString(objectValues), ex); 
        }
        
        if (existingVersion == null) {
            return true;
        } else {
            Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                    CsvData.ROW_DATA);
            Long loadingVersion = Long.valueOf(newData.get(columnName));
            return loadingVersion > existingVersion;
        }
    }

}
