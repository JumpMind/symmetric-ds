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
package org.jumpmind.db.platform.db2;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Db2zOsDmlStatement extends DmlStatement {
    
   private static final FastDateFormat DATE_FORMATTER = FastDateFormat
            .getInstance("yyyy-MM-dd");
   
   protected static final Logger log = LoggerFactory.getLogger(Db2zOsDmlStatement.class);

    public Db2zOsDmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keysColumns, Column[] columns,
            boolean[] nullKeyValues, DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, nullKeyValues, databaseInfo, useQuotedIdentifiers,
                textColumnExpression);
    }
    
    @Override
    public String buildDynamicSql(BinaryEncoding encoding, Row row,
            boolean useVariableDates, boolean useJdbcTimestampFormat, Column[] columns) {
        
        if (useJdbcTimestampFormat) {
            log.debug("zOS doesn't support useJdbcTimestampFormat.  Changing to false.");
            useJdbcTimestampFormat = false;
        }
        final String QUESTION_MARK = "<!QUESTION_MARK!>";
        String newSql = sql;
        String quote = databaseInfo.getValueQuoteToken();
        String binaryQuoteStart = databaseInfo.getBinaryQuoteStart();
        String binaryQuoteEnd = databaseInfo.getBinaryQuoteEnd();
        String regex = "\\?";
        
        List<Column> columnsToProcess = new ArrayList<Column>();
        columnsToProcess.addAll(Arrays.asList(columns));
        
        for (int i = 0; i < columnsToProcess.size(); i++) {
            Column column = columnsToProcess.get(i);
            String name = column.getName();
            int type = column.getMappedTypeCode();

            if (row.get(name) != null) {
                if (column.isOfTextType()) {
                    try {
                        String value = row.getString(name);
                        value = value.replace("\\", "\\\\");
                        value = value.replace("$", "\\$");
                        value = value.replace("'", "''");
                        value = value.replace("?", QUESTION_MARK);
                        newSql = newSql.replaceFirst(regex, quote + value + quote);
                    } catch (RuntimeException ex) {
                        log.error("Failed to replace ? in {" + sql + "} with " + name + "="
                                + row.getString(name));
                        throw ex;
                    }
                } else if (column.isTimestampWithTimezone()) {
                    newSql = newSql.replaceFirst(regex, quote + row.getString(name) + quote);
                } else if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
                    Date date = row.getDateTime(name);
                    if (useVariableDates) {
                        long diff = date.getTime() - System.currentTimeMillis();
                        newSql = newSql.replaceFirst(regex, "${curdate" + diff + "}");
                    } else if (type == Types.TIME) {
                        newSql = newSql.replaceFirst(regex, (useJdbcTimestampFormat ? "{ts " : "")
                                + quote + FormatUtils.TIME_FORMATTER.format(date) + quote
                                + (useJdbcTimestampFormat ? "}" : ""));
                    } else if (type == Types.DATE) { // Handle date as just 'YYYY-MM-DD' for zOS.
                        newSql = newSql.replaceFirst(regex, (useJdbcTimestampFormat ? "{ts " : "")
                                + quote + DATE_FORMATTER.format(date) + quote
                                + (useJdbcTimestampFormat ? "}" : ""));
                    } else {
                        newSql = newSql.replaceFirst(regex, (useJdbcTimestampFormat ? "{ts " : "")
                                + quote + FormatUtils.TIMESTAMP_FORMATTER.format(date) + quote
                                + (useJdbcTimestampFormat ? "}" : ""));
                    }
                } else if (column.isOfBinaryType()) {
                    byte[] bytes = row.getBytes(name);
                    if (encoding == BinaryEncoding.NONE) {
                        newSql = newSql.replaceFirst(regex, quote + row.getString(name));
                    } else if (encoding == BinaryEncoding.BASE64) {
                        newSql = newSql.replaceFirst(regex,
                                quote + new String(Base64.encodeBase64(bytes)) + quote);
                    } else if (encoding == BinaryEncoding.HEX) {
                        newSql = newSql.replaceFirst(regex, binaryQuoteStart
                                + new String(Hex.encodeHex(bytes)) + binaryQuoteEnd);
                    }
                } else {
                    newSql = newSql.replaceFirst(regex, row.getString(name));
                }
            } else {
                newSql = newSql.replaceFirst(regex, "null");
            }
        }
        
        newSql = newSql.replace(QUESTION_MARK, "?");
        return newSql + databaseInfo.getSqlCommandDelimiter();  
    }  
    
    @Override
    protected void appendColumnNameForSql(StringBuilder sql, Column column, boolean select) {
        String columnName = column.getName();        
        
        if (select && column.isPrimaryKey() && column.isOfTextType()) {
            // CAST to ASCII to support standard ORDER BY ordering.
            String quotedColumn = quote+columnName+quote;
            String typeName = column.getJdbcTypeName();
            String size = column.getSize();
            sql.append("CAST(").append(quotedColumn).append(" AS ").append(typeName) 
                    .append("(").append(size).append(")CCSID ASCII) AS ").append(quotedColumn);
        } else {            
            sql.append(quote).append(columnName).append(quote);
        }
    }

}
