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

import java.util.LinkedHashMap;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.ISymmetricEngine;

public class DbCompareRow {
    
    private DbValueComparator dbValueComparator;
    private ISymmetricEngine engine;
    private Table table;
    private Row row;
    private Map<String, String> rowValues;
    
    public DbCompareRow(ISymmetricEngine engine, DbValueComparator dbValueComparator, Table table, Row row) {
        this.engine = engine;
        this.table = table;
        this.row = row;
        this.dbValueComparator = dbValueComparator;
        this.rowValues = loadRowValues();
    }
    
    public int comparePks(DbCompareRow targetRow) {
        Table targetTable = targetRow.getTable();

        for (int i = 0; i < table.getPrimaryKeyColumnCount(); i++) {
            String sourcePkColumn = table.getPrimaryKeyColumnNames()[i];
            String targetPkColumn = targetTable.getPrimaryKeyColumnNames()[i];

            int result = dbValueComparator.compareValues(table.getPrimaryKeyColumns()[i], targetTable.getPrimaryKeyColumns()[i], 
                    rowValues.get(sourcePkColumn), targetRow.getRowValues().get(targetPkColumn));
            
            if (result != 0) {
                return result;
            }
        }    
        return 0;
    }
    
    public Map<Column, String> compareTo(DbCompareRow targetRow) {
        
        Map<Column, String> deltas = new LinkedHashMap<Column, String>();
        // TODO maybe should operate on non-pk columns here.
        for (int i = 0; i < table.getColumnCount(); i++) {
            Table targetTable = targetRow.getTable();
            
            String sourceColumn = table.getColumnNames()[i];
            String targetColumn = targetTable.getColumnNames()[i];
            
            int result = dbValueComparator.compareValues(table.getColumns()[i], targetTable.getColumns()[i], 
                    rowValues.get(sourceColumn), targetRow.getRowValues().get(targetColumn));
            
            if (result != 0) {
                deltas.put(targetTable.getColumns()[i], rowValues.get(sourceColumn));
            }            
        }
        
        return deltas;
    }
    
    protected Map<String, String> loadRowValues() {
        String[] stringValues = engine.getDatabasePlatform().
                getStringValues(BinaryEncoding.HEX, table.getColumns(), row, false, false);
        
        Map<String, String> localRowValues = new LinkedHashMap<String, String>();
        
        for (int i = 0; i < stringValues.length; i++) {
            String columnName;
            if (i < table.getColumnCount()) {
                columnName = table.getColumn(i).getName();
                String stringValue = stringValues[i];
                localRowValues.put(columnName, stringValue);
            } 
        }
        
        return localRowValues;

    }

    public DbValueComparator getDbValueComparator() {
        return dbValueComparator;
    }

    public void setDbValueComparator(DbValueComparator dbValueComparator) {
        this.dbValueComparator = dbValueComparator;
    }

    public ISymmetricEngine getEngine() {
        return engine;
    }

    public void setEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    public Map<String, String> getRowValues() {
        return rowValues;
    }

    public void setRowValues(Map<String, String> rowValues) {
        this.rowValues = rowValues;
    }


}
