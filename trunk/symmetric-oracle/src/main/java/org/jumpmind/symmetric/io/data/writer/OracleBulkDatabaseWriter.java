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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import oracle.jdbc.OracleTypes;
import oracle.jdbc.internal.OracleCallableStatement;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.BulkSqlException;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class OracleBulkDatabaseWriter extends DatabaseWriter {

    protected String procedurePrefix;

    protected NativeJdbcExtractor jdbcExtractor;

    protected int maxRowsBeforeFlush;

    protected DataEventType lastEventType;

    protected List<List<Object>> rowArrays = new ArrayList<List<Object>>();

    public OracleBulkDatabaseWriter(IDatabasePlatform platform, String procedurePrefix,
            NativeJdbcExtractor jdbcExtractor, int maxRowsBeforeFlush) {
        super(platform);
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
                Object[] rowData = platform.getObjectValues(batch.getBinaryEncoding(),
                        getRowData(data), targetTable.getColumns());
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
                break;
            case UPDATE:
                super.write(data);
                break;
            case DELETE:
                super.write(data);
                break;
            default:
                super.write(data);
                break;
        }

        if (requiresFlush) {
            flush();
        }
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
                int[] errors = errorsArray.getIntArray();

                if (errors.length > 0) {
                    // set the statement count so the failed row number get reported correctly
                    statistics.get(batch).set(DataWriterStatisticConstants.STATEMENTCOUNT,
                            errors[0]);

                    throw new BulkSqlException(errors, lastEventType.toString(), sql);
                }
            }

        } catch (SQLException ex) {
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
            case Types.CHAR:
            case Types.VARCHAR:
                return "varchar(4000)";
            case Types.DATE:
            case Types.TIME:
            case OracleTypes.TIMESTAMPTZ:
                return "timestamp with time zone";
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
            case Types.CHAR:
            case Types.VARCHAR:
                return String.format("%s_%s_t", procedurePrefix, "varchar").toUpperCase();
            case Types.DATE:
            case Types.TIME:
            case OracleTypes.TIMESTAMPTZ:
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
                throw new UnsupportedOperationException(Integer.toString(typeCode));

        }
    }

    protected List<Column> getBulkLoadableColumns(Table table) {
        ArrayList<Column> columns = new ArrayList<Column>(Arrays.asList(table.getColumns()));
        Iterator<Column> iterator = columns.iterator();
        // TODO support BLOB and CLOBs in bulk load. For now, remove them
        while (iterator.hasNext()) {
            Column column = (Column) iterator.next();
            if (column.getMappedTypeCode() == Types.CLOB || column.getMappedTypeCode() == Types.BLOB
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
            ddl.append(String.format("  insert into %s (\n", table.getFullyQualifiedTableName("\"")));
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
