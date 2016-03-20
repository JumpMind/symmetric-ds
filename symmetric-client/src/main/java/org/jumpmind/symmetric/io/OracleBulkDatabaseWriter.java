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
package org.jumpmind.symmetric.io;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.BulkSqlException;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DefaultDatabaseWriter;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

import oracle.jdbc.OracleTypes;
import oracle.jdbc.internal.OracleCallableStatement;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

public class OracleBulkDatabaseWriter extends DefaultDatabaseWriter {

    protected String procedurePrefix;

    protected NativeJdbcExtractor jdbcExtractor;

    protected int maxRowsBeforeFlush;

    protected DataEventType lastEventType;

    protected List<List<Object>> rowArrays = new ArrayList<List<Object>>();

    public OracleBulkDatabaseWriter(IDatabasePlatform platform, String procedurePrefix,
            NativeJdbcExtractor jdbcExtractor, int maxRowsBeforeFlush, DatabaseWriterSettings settings) {
        super(platform, settings);
        this.procedurePrefix = procedurePrefix;
        this.jdbcExtractor = jdbcExtractor;
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
    }

    public boolean start(Table table) {
        if (super.start(table)) {
            // TODO come up with smarter way to build procedures
            buildBulkInsertProcedure(targetTable);
            return true;
        } else {
            return false;
        }
    }

    public void write(CsvData data) {
        DataEventType dataEventType = data.getDataEventType();

        if (lastEventType != null && !lastEventType.equals(dataEventType)) {
            flush();
        }

        lastEventType = dataEventType;

        boolean requiresFlush = false;
        switch (dataEventType) {
            case INSERT:
                statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
                statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                if (filterBefore(data)) {
                    Object[] rowData = platform.getObjectValues(batch.getBinaryEncoding(), getRowData(data, CsvData.ROW_DATA),
                            targetTable.getColumns(), false, writerSettings.isFitToColumn());

                    rowData = convertObjectValues(rowData, targetTable.getColumns());

                    for (int i = 0; i < rowData.length; i++) {

                        List<Object> columnList = null;
                        if (rowArrays.size() > i) {
                            columnList = rowArrays.get(i);
                        } else {
                            columnList = new ArrayList<Object>();
                            rowArrays.add(columnList);
                        }
                        columnList.add(rowData[i]);

                        if (columnList.size() >= maxRowsBeforeFlush) {
                            requiresFlush = true;
                        }
                    }
                    uncommittedCount++;
                }
                break;
            case UPDATE:
                flush();
                super.write(data);
                break;
            case DELETE:
                flush();
                super.write(data);
                break;
            default:
                flush();
                super.write(data);
                break;
        }

        if (requiresFlush) {
            flush();
        }

        checkForEarlyCommit();
    }

    /**
     * @param rowData
     * @param columns
     * @return
     */
    protected Object[] convertObjectValues(Object[] values, Column[] columns) {
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                Column column = columns.length > i ? columns[i] : null;
                if (column != null) {
                    values[i] = convertObjectValue(value, column);
                }
            }
        }

        return values;
    }

    /**
     * @param value
     * @param column
     * @return
     */
    protected Object convertObjectValue(Object value, Column column) {
        if (value == null) {
            return null;
        }

        if (column.getMappedTypeCode() == OracleTypes.TIMESTAMPTZ
                || column.getMappedTypeCode() == OracleTypes.TIMESTAMPLTZ) {
            String stringValue = (String)value;
            return parseTimestampTZ(column.getMappedTypeCode(), stringValue);
        }

        return value;
    }

    protected Datum parseTimestampTZ(int type, String value) {
        if (value == null || StringUtils.isEmpty(value.trim())) {
            return null;
        }

        try {
            Timestamp timestamp = null;
            TimeZone timezone = null;
            try {
                // Try something like: 2015-11-20 13:37:44.000000000
                timestamp = Timestamp.valueOf(value.trim());
                timezone = TimeZone.getDefault();
            }
            catch (Exception ex) {
                log.debug("Failed to convert value to timestamp.", ex);
                // Now expecting something like: 2015-11-20 13:37:44.000000000 +08:00
                int split = value.lastIndexOf(" ");
                String timestampString = value.substring(0, split).trim();
                if (timestampString.endsWith(".")) { // Cover case where triggers would export format like "2007-01-02 03:20:10."
                    timestampString = timestampString.substring(0, timestampString.length()-1);
                }
                String timezoneString = value.substring(split).trim();

                timestamp = Timestamp.valueOf(timestampString);

                timezone = ((AbstractDatabasePlatform)platform).getTimeZone(timezoneString);
                // Even though we provide the timezone to the Oracle driver, apparently 
                // the timestamp component needs to actually be in UTC.
                if (type == OracleTypes.TIMESTAMPTZ) {
                    timestamp = toUTC(timestamp, timezone);                
                }
            }
            Calendar timezoneCalender = Calendar.getInstance();
            timezoneCalender.setTimeZone(timezone);
            timezoneCalender.setTime(timestamp);

            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
            Connection c = jdbcTransaction.getConnection();

            Connection oracleConnection = jdbcExtractor.getNativeConnection(c);
            Datum ts = null;
            if (type == OracleTypes.TIMESTAMPTZ) {
                ts = new TIMESTAMPTZ(oracleConnection, timestamp, timezoneCalender);
            } else {
                ts = new TIMESTAMPLTZ(oracleConnection, timestamp);
            }
            return ts;
        } catch (Exception ex) {
            log.info("Failed to convert '" + value + "' to TIMESTAMPTZ." );
            throw platform.getSqlTemplate().translate(ex);
        }
    }    

    public Timestamp toUTC(Timestamp timestamp, TimeZone timezone) {       
        int nanos = timestamp.getNanos();
        timestamp.setNanos(0);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        dateFormat.setTimeZone(timezone);
        Date date;
        try {
            date = dateFormat.parse(timestamp.toString());
        } catch (ParseException ex) {
            log.info("Failed to parse '" + timestamp + "'");
            throw platform.getSqlTemplate().translate(ex);
        }

        Timestamp utcTimestamp = new Timestamp(date.getTime());
        utcTimestamp.setNanos(nanos);
        return utcTimestamp;
    }

    protected void flush() {
        statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        try {
            if (rowArrays.size() > 0) {
                JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
                Connection c = jdbcTransaction.getConnection();
                Connection oracleConnection = jdbcExtractor.getNativeConnection(c);

                Column[] columns = targetTable.getColumns();
                StringBuilder questions = new StringBuilder();
                for (int i = 0; i <= columns.length; i++) {
                    questions.append("?, ");
                }
                questions.replace(questions.length() - 2, questions.length(), "");

                String sql = String.format("{ call %s(%s) }", buildProcedureName("i", targetTable),
                        questions);
                OracleCallableStatement stmt = (OracleCallableStatement) oracleConnection
                        .prepareCall(sql);

                for (int i = 0; i < columns.length; i++) {
                    Column column = columns[i];
                    ArrayDescriptor type = ArrayDescriptor.createDescriptor(
                            getTypeName(column.getMappedTypeCode()), oracleConnection);
                    List<Object> columnData = rowArrays.get(i);
                    ARRAY array = new ARRAY(type, oracleConnection,
                            columnData.toArray(new Object[columnData.size()]));
                    stmt.setObject(i + 1, array);
                }

                int errorIndex = columns.length + 1;
                stmt.registerOutParameter(errorIndex, OracleTypes.ARRAY, getTypeName(Types.INTEGER));

                stmt.execute();

                ARRAY errorsArray = stmt.getARRAY(errorIndex);
                int[] errors;
                if (errorsArray != null) {
                    errors = errorsArray.getIntArray();
                } else {
                    errors = new int[0];
                }
                
                try{ 
                	stmt.close();
                }
                catch (SQLException e) {
                	log.info("Unable to close the prepared statment after user.", e);
                }
                
                if (errors.length > 0) {
                    // set the statement count so the failed row number get reported correctly
                    statistics.get(batch).set(DataWriterStatisticConstants.STATEMENTCOUNT,
                            errors[0]);

                    throw new BulkSqlException(errors, lastEventType.toString(), sql);
                }
            }

        } catch (SQLException ex) {
            StringBuilder failureMessage = new StringBuilder();
            failureMessage.append("Failed to flush the following data set from batch ");
            failureMessage.append(batch.getNodeBatchId());
            failureMessage.append(".  The last flushed line number of the batch was ");
            failureMessage.append(statistics.get(batch).get(DataWriterStatisticConstants.LINENUMBER));
            failureMessage.append("\n");

            for (List<Object> row : rowArrays) {
                failureMessage.append(StringUtils.abbreviate(Arrays.toString(row.toArray(new Object[row.size()])), CsvData.MAX_DATA_SIZE_TO_PRINT_TO_LOG));
                failureMessage.append("\n");
            }
            log.info(failureMessage.toString());
            throw platform.getSqlTemplate().translate(ex);
        } finally {
            lastEventType = null;
            rowArrays.clear();
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

    @Override
    public void end(Table table) {
        flush();
        super.end(table);
    }

    protected void buildBulkDataType(int typeCode) {
        String typeName = getTypeName(typeCode);
        if (platform.getSqlTemplate().queryForInt(
                "select count(*) from user_types where type_name=?", typeName) == 0) {
            final String DDL = "create or replace type %s is table of %s";
            platform.getSqlTemplate().update(
                    String.format(DDL, getTypeName(typeCode), getMappedType(typeCode)));
        }
    }

    protected String getMappedType(int typeCode) {
        switch (typeCode) {
            case Types.CLOB:
            case Types.NCLOB:
                return "clob";
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return "varchar(4000)";
            case OracleTypes.TIMESTAMPTZ:
                return "timestamp(9) with time zone";
            case OracleTypes.TIMESTAMPLTZ:
                return "timestamp(9) with local time zone";                
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.BIGINT:
            case Types.DOUBLE:
                return "number";
            case Types.BIT:
            case Types.TINYINT:
            case Types.INTEGER:
                return "integer";
            default:
                throw new UnsupportedOperationException();

        }
    }

    protected String getTypeName(int typeCode) {
        switch (typeCode) {
            case Types.CLOB:
            case Types.NCLOB:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return String.format("%s_%s_t", procedurePrefix, "varchar").toUpperCase();
            case OracleTypes.TIMESTAMPTZ:
                return String.format("%s_%s_t", procedurePrefix, "timestamptz").toUpperCase();
            case OracleTypes.TIMESTAMPLTZ:
                return String.format("%s_%s_t", procedurePrefix, "timestampltz").toUpperCase();
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return String.format("%s_%s_t", procedurePrefix, "timestamp").toUpperCase();
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.BIGINT:
            case Types.DOUBLE:
                return String.format("%s_%s_t", procedurePrefix, "number").toUpperCase();
            case Types.BIT:
            case Types.TINYINT:
            case Types.INTEGER:
                return String.format("%s_%s_t", procedurePrefix, "integer").toUpperCase();
            default:
                throw new UnsupportedOperationException("OracleBulkDatabaseWriter does not support type: " + Integer.toString(typeCode));

        }
    }

    protected List<Column> getBulkLoadableColumns(Table table) {
        ArrayList<Column> columns = new ArrayList<Column>(Arrays.asList(table.getColumns()));
        Iterator<Column> iterator = columns.iterator();
        // TODO support BLOB and CLOBs in bulk load. For now, remove them
        while (iterator.hasNext()) {
            Column column = (Column) iterator.next();
            if (column.getMappedTypeCode() == Types.BLOB
                    || column.getMappedTypeCode() == Types.VARBINARY) {
                iterator.remove();
            }
        }
        return columns;
    }

    protected String buildProcedureName(String dmlAbbrev, Table table) {
        return String.format("%s_%s_%s", procedurePrefix, dmlAbbrev,
                Math.abs(table.calculateTableHashcode())).toUpperCase();
    }

    protected void buildBulkInsertProcedure(Table table) {
        String procedureName = buildProcedureName("i", table);
        if (platform.getSqlTemplate().queryForInt(
                "select count(*) from user_procedures where object_name=?", procedureName) == 0) {
            List<Column> columns = getBulkLoadableColumns(table);
            // needed for error codes
            buildBulkDataType(Types.INTEGER);
            for (Column column : columns) {
                buildBulkDataType(column.getMappedTypeCode());
            }
            // @formatter:off        
            StringBuilder ddl = new StringBuilder();
            ddl.append("create or replace  \n");
            ddl.append("procedure          \n");        
            ddl.append(procedureName);
            ddl.append(" (\n");
            String firstVariable = null;
            for (Column column : columns) {
                String variable = String.format("i_%s", column.getName().toLowerCase());
                if (firstVariable == null) {
                    firstVariable = variable;
                }
                ddl.append(String.format("%s %s, \n", variable, getTypeName(column.getMappedTypeCode())));
            }
            ddl.append(String.format("o_errors out %s_integer_t)\n", procedurePrefix));
            ddl.append(String.format(" is                                                                         \n"));
            ddl.append(String.format(" dml_errors EXCEPTION;                                                      \n"));
            ddl.append(String.format(" PRAGMA EXCEPTION_INIT(dml_errors, -24381);                                 \n"));
            ddl.append(String.format("begin                                                                       \n"));
            ddl.append(String.format("  o_errors := %s_integer_t();                                                  \n", procedurePrefix));
            ddl.append(String.format("  forall i in 1 .. %s.last save exceptions                                  \n", firstVariable));
            ddl.append(String.format("  insert into %s (\n", table.getQualifiedTableName("\"", ".", ".")));
            for (Column column : columns) {
                ddl.append(String.format("\"%s\", \n", column.getName()));
            }
            ddl.replace(ddl.length()-3, ddl.length(), ")\n");
            ddl.append(String.format("                      values (                                              \n"));
            for (Column column : columns) {
                String variable = String.format("i_%s", column.getName().toLowerCase());
                ddl.append(String.format("%s(i), \n", variable));
            }
            ddl.replace(ddl.length()-3, ddl.length(), ");\n");

            ddl.append(String.format("exception                                                                   \n"));
            ddl.append(String.format("  when dml_errors then                                                      \n"));
            ddl.append(String.format("    for i in 1 .. SQL%%BULK_EXCEPTIONS.count loop                            \n"));
            ddl.append(String.format("      if SQL%%BULK_EXCEPTIONS(i).ERROR_CODE != 00001 then                  \n"));       
            ddl.append(String.format("        raise;                                                            \n"));       
            ddl.append(String.format("      end if;                                                             \n"));       
            ddl.append(String.format("      o_errors.extend(1);                                                   \n"));       
            ddl.append(String.format("      o_errors(o_errors.count) := SQL%%BULK_EXCEPTIONS(i).ERROR_INDEX;       \n"));       
            ddl.append(String.format("    end loop;                                                               \n"));       
            ddl.append(String.format("end %s;                                                      ", procedureName));

            if (log.isDebugEnabled()) {
                log.debug(ddl.toString());
            }
            platform.getSqlTemplate().update(ddl.toString());
        }
    }   

}
