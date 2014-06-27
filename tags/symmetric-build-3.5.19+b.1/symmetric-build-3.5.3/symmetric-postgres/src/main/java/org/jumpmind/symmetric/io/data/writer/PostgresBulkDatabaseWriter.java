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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class PostgresBulkDatabaseWriter extends DatabaseWriter {

    protected NativeJdbcExtractor jdbcExtractor;

    protected int maxRowsBeforeFlush;

    protected CopyManager copyManager;

    protected CopyIn copyIn;

    protected int loadedRows = 0;

    public PostgresBulkDatabaseWriter(IDatabasePlatform platform,
            NativeJdbcExtractor jdbcExtractor, int maxRowsBeforeFlush) {
        super(platform);
        this.jdbcExtractor = jdbcExtractor;
        this.maxRowsBeforeFlush = maxRowsBeforeFlush;
    }

    public void write(CsvData data) {
        DataEventType dataEventType = data.getDataEventType();

        switch (dataEventType) {
            case INSERT:
                startCopy();
                statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
                statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                try {
                    String formattedData = CsvUtils.escapeCsvData(
                            data.getParsedData(CsvData.ROW_DATA), '\n', '\'', 
                            CsvWriter.ESCAPE_MODE_DOUBLED);
                    byte[] dataToLoad = formattedData.getBytes();
                    copyIn.writeToCopy(dataToLoad, 0, dataToLoad.length);
                    loadedRows++;
                } catch (Exception ex) {
                    throw getPlatform().getSqlTemplate().translate(ex);
                } finally {
                    statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                }
                break;
            case UPDATE:
            case DELETE:
            default:
                endCopy();
                super.write(data);
                break;
        }

        if (loadedRows >= maxRowsBeforeFlush) {
            flush();
            loadedRows = 0;
        }
    }

    protected void flush() {
        if (copyIn != null) {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            try {
                copyIn.flushCopy();
            } catch (SQLException ex) {
                throw getPlatform().getSqlTemplate().translate(ex);
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }
        }
    }

    @Override
    public void open(DataContext context) {
        super.open(context);
        try {
            JdbcSqlTransaction jdbcTransaction = (JdbcSqlTransaction) transaction;
            Connection conn = jdbcExtractor.getNativeConnection(jdbcTransaction.getConnection());
            copyManager = new CopyManager((BaseConnection) conn);
        } catch (Exception ex) {
            throw getPlatform().getSqlTemplate().translate(ex);
        }
    }

    protected void startCopy() {
        if (copyIn == null && targetTable != null) {
            try {
                String sql = createCopyMgrSql();
                if (log.isDebugEnabled()) {
                    log.debug("starting bulk copy using: {}", sql);
                }
                copyIn = copyManager.copyIn(sql);
            } catch (Exception ex) {
                throw getPlatform().getSqlTemplate().translate(ex);
            }
        }
    }

    protected void endCopy() {
        if (copyIn != null) {
            try {
                flush();
            } finally {
                try {
                    copyIn.endCopy();
                } catch (Exception ex) {
                    throw getPlatform().getSqlTemplate().translate(ex);
                } finally {
                    copyIn = null;
                }
            }
        }
    }

    @Override
    public void end(Table table) {
        try {
            endCopy();
        } finally {
            super.end(table);
        }
    }

    @Override
    public void end(Batch batch, boolean inError) {
        if (inError && copyIn != null) {
            try {
                copyIn.cancelCopy();
            } catch (SQLException e) {
            } finally {
                copyIn = null;
            }
        }
        super.end(batch, inError);
    }

    private String createCopyMgrSql() {
        StringBuilder sql = new StringBuilder("COPY ");
        String quote = platform.getDatabaseInfo().getDelimiterToken();
        sql.append(targetTable.getFullyQualifiedTableName(quote));
        sql.append("(");
        Column[] columns = targetTable.getColumns();

        for (Column column : columns) {
            String columnName = column.getName();
            if (StringUtils.isNotBlank(columnName)) {
                sql.append(quote);
                sql.append(columnName);
                sql.append(quote);
                sql.append(",");
            }
        }
        sql.replace(sql.length() - 1, sql.length(), ")");
        sql.append("FROM STDIN with delimiter ',' csv quote ''''");
        return sql.toString();
    }

}
