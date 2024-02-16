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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbCompareDiffWriter {
    private final static Logger log = LoggerFactory.getLogger(DbCompareDiffWriter.class);
    // Continue after parsing errors
    private boolean continueAfterError = false;
    private boolean error = false;
    private Throwable throwable = null;

    public DbCompareDiffWriter(ISymmetricEngine targetEngine, DbCompareTables tables, OutputStream stream) {
        super();
        this.targetEngine = targetEngine;
        this.tables = tables;
        this.stream = stream;
    }

    private ISymmetricEngine targetEngine;
    private DbCompareTables tables;
    private OutputStream stream;

    public void writeDelete(DbCompareRow targetCompareRow) {
        if (stream == null) {
            return;
        }
        try {
            Table table = targetCompareRow.getTable();
            DmlStatement statement = targetEngine.getDatabasePlatform().createDmlStatement(DmlType.DELETE,
                    table.getCatalog(), table.getSchema(), table.getName(),
                    table.getPrimaryKeyColumns(), null,
                    null, null);
            Row row = new Row(targetCompareRow.getTable().getPrimaryKeyColumnCount());
            for (int i = 0; i < targetCompareRow.getTable().getPrimaryKeyColumnCount(); i++) {
                row.put(table.getColumn(i).getName(),
                        targetCompareRow.getRowValues().get(targetCompareRow.getTable().getColumn(i).getName()));
            }
            String sql = statement.buildDynamicDeleteSql(BinaryEncoding.HEX, row, false, true);
            writeLine(sql);
        } catch (RuntimeException e) {
            error = true;
            throwable = e;
            log.error(e.getMessage(), e);
            if (!isContinueAfterError()) {
                throw e;
            }
        }
    }

    public void writeInsert(DbCompareRow sourceCompareRow) {
        if (stream == null) {
            return;
        }
        try {
            Table targetTable = tables.getTargetTable();
            List<Column> targetColumns = new ArrayList<Column>();
            List<Column> targetPkColumns = new ArrayList<Column>();
            for (Column targetColumn : targetTable.getColumns()) {
                if (tables.getColumnMapping().containsValue(targetColumn) || !targetColumn.isRequired()
                        || targetColumn.getDefaultValue() == null) {
                    targetColumns.add(targetColumn);
                    if (targetColumn.isPrimaryKey()) {
                        targetPkColumns.add(targetColumn);
                    }
                }
            }
            DmlStatement statement = targetEngine.getDatabasePlatform().createDmlStatement(DmlType.INSERT,
                    targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                    targetPkColumns.toArray(new Column[targetPkColumns.size()]),
                    targetColumns.toArray(new Column[targetColumns.size()]), null, null);
            Row row = new Row(targetColumns.size());
            for (Column sourceColumn : tables.getSourceTable().getColumns()) {
                Column targetColumn = tables.getColumnMapping().get(sourceColumn);
                if (targetColumn == null) {
                    continue;
                }
                row.put(targetColumn.getName(), sourceCompareRow.getRowValues().get(sourceColumn.getName()));
            }
            String sql = statement.buildDynamicSql(BinaryEncoding.HEX, row, false, false);
            writeLine(sql);
        } catch (RuntimeException e) {
            error = true;
            throwable = e;
            log.error(e.getMessage(), e);
            if (!isContinueAfterError()) {
                throw e;
            }
        }
    }

    public void writeUpdate(DbCompareRow targetCompareRow, Map<Column, String> deltas) {
        if (stream == null) {
            return;
        }
        try {
            Table table = targetCompareRow.getTable();
            Column[] changedColumns = deltas.keySet().toArray(new Column[deltas.keySet().size()]);
            DmlStatement statement = targetEngine.getDatabasePlatform().createDmlStatement(DmlType.UPDATE,
                    table.getCatalog(), table.getSchema(), table.getName(),
                    table.getPrimaryKeyColumns(), changedColumns,
                    null, null);
            Row row = new Row(changedColumns.length + table.getPrimaryKeyColumnCount());
            for (Column changedColumn : deltas.keySet()) {
                String value = deltas.get(changedColumn);
                row.put(changedColumn.getName(), value);
            }
            for (String pkColumnName : table.getPrimaryKeyColumnNames()) {
                String value = targetCompareRow.getRow().getString(pkColumnName);
                row.put(pkColumnName, value);
            }
            String sql = statement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
            writeLine(sql);
        } catch (RuntimeException e) {
            error = true;
            throwable = e;
            log.error(e.getMessage(), e);
            if (!isContinueAfterError()) {
                throw e;
            }
        }
    }

    public void close() {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                log.debug("CAUGHT EXCEPTION while closing stream", e);
            }
            stream = null;
        }
    }

    protected void writeLine(String line) {
        try {
            stream.write(line.getBytes());
            stream.write("\r\n".getBytes());
        } catch (Exception ex) {
            throw new RuntimeException("failed to write to stream '" + line + "'", ex);
        }
    }

    public void setContinueAfterError(boolean continueAfterError) {
        this.continueAfterError = continueAfterError;
    }

    public boolean isContinueAfterError() {
        return continueAfterError;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public boolean isError() {
        return error;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
