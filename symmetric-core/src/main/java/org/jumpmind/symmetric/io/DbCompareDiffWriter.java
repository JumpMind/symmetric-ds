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

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
    
    final static Logger log = LoggerFactory.getLogger(DbCompareDiffWriter.class);
    
    public DbCompareDiffWriter(ISymmetricEngine targetEngine, DbCompareTables tables, String fileName) {
        super();
        this.targetEngine = targetEngine;
        this.tables = tables;
        this.fileName = getFormattedFileName(fileName);
    }

    private ISymmetricEngine targetEngine;
    private DbCompareTables tables;
    private String fileName;
    private FileOutputStream stream;
    
    public void writeDelete(DbCompareRow targetCompareRow) {
        stream = initStreamIfNeeded(stream, fileName);
        if (stream == null) {
            return;
        }

        Table table = targetCompareRow.getTable();

        DmlStatement statement =  targetEngine.getDatabasePlatform().createDmlStatement(DmlType.DELETE,
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
    }

    public void writeInsert(DbCompareRow sourceCompareRow) { 
        stream = initStreamIfNeeded(stream, fileName);
        if (stream == null) {
            return;
        }

        Table targetTable = tables.getTargetTable();

        DmlStatement statement =  targetEngine.getDatabasePlatform().createDmlStatement(DmlType.INSERT,
                targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                targetTable.getPrimaryKeyColumns(), targetTable.getColumns(),
                null, null);

        Row row = new Row(targetTable.getColumnCount());

        for (Column sourceColumn : tables.getSourceTable().getColumns()) {
            Column targetColumn = tables.getColumnMapping().get(sourceColumn);
            if (targetColumn == null) {
                continue;
            }

            row.put(targetColumn.getName(), sourceCompareRow.getRowValues().
                    get(sourceColumn.getName()));
        }

        String sql = statement.buildDynamicSql(BinaryEncoding.HEX, row, false, false);

        writeLine(sql);
    }

    public void writeUpdate(DbCompareRow targetCompareRow, Map<Column, String> deltas) { 
        stream = initStreamIfNeeded(stream, fileName);
        if (stream == null) {
            return;
        }

        Table table = targetCompareRow.getTable();

        Column[] changedColumns = deltas.keySet().toArray(new Column[deltas.keySet().size()]);

        DmlStatement statement = targetEngine.getDatabasePlatform().createDmlStatement(DmlType.UPDATE,
                table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), changedColumns,
                null, null);

        Row row = new Row(changedColumns.length+table.getPrimaryKeyColumnCount());
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
    
    protected String getFormattedFileName(String intputFileName) {
        if (!StringUtils.isEmpty(intputFileName)) {
            // allow file per table.
            String fileNameFormatted = intputFileName.replace("%t", "%s");
            fileNameFormatted = String.format(fileNameFormatted, tables.getSourceTable().getName());   
            fileNameFormatted = fileNameFormatted.replaceAll("\"", "").replaceAll("\\]", "").replaceAll("\\[", "");
            return fileNameFormatted;
        } else {
            return null;
        }
    }    
    
    protected FileOutputStream initStreamIfNeeded(FileOutputStream diffStream, String fileName) {
        if (diffStream != null) {
            return diffStream;
        } else {
            log.info("Writing diffs to {}", fileName);
            try {                
                return new FileOutputStream(fileName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to open stream to file '" + fileName + "'", e);
            }
        }
    }    


}
