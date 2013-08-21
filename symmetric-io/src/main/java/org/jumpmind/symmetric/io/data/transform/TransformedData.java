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
package org.jumpmind.symmetric.io.data.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.transform.TransformColumn.IncludeOnType;

public class TransformedData implements Cloneable {

    protected boolean generatedIdentityNeeded = false;

    protected DataEventType targetDmlType;

    protected DataEventType sourceDmlType;

    protected Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> columnsBy;

    protected Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> keysBy;

    protected TransformTable transformation;

    protected Map<String, String> sourceKeyValues;

    protected Map<String, String> oldSourceValues;

    protected Map<String, String> sourceValues;

    public TransformedData(TransformTable transformation, DataEventType sourceDmlType,
            Map<String, String> sourceKeyValues, Map<String, String> oldSourceValues,
            Map<String, String> sourceValues) {
        this.transformation = transformation;
        this.targetDmlType = sourceDmlType;
        this.sourceDmlType = sourceDmlType;
        this.sourceKeyValues = sourceKeyValues;
        this.oldSourceValues = oldSourceValues;
        this.sourceValues = sourceValues;
    }

    public String getFullyQualifiedTableName() {
        return transformation.getFullyQualifiedTargetTableName();
    }

    public DataEventType getTargetDmlType() {
        return targetDmlType;
    }

    public void setTargetDmlType(DataEventType dmlType) {
        this.targetDmlType = dmlType;
    }

    public String getTableName() {
        return transformation.getTargetTableName();
    }

    public String getCatalogName() {
        return transformation.getTargetCatalogName();
    }

    public String getSchemaName() {
        return transformation.getTargetSchemaName();
    }

    public void put(TransformColumn column, String columnValue, boolean recordAsKey) {
        if (recordAsKey) {
            if (keysBy == null) {
                keysBy = new HashMap<TransformColumn.IncludeOnType, LinkedHashMap<String, String>>(
                        2);
            }
            LinkedHashMap<String, String> keyValues = keysBy.get(column.getIncludeOn());
            if (keyValues == null) {
                keyValues = new LinkedHashMap<String, String>();
                keysBy.put(column.getIncludeOn(), keyValues);
            }
            keyValues.put(column.getTargetColumnName(), columnValue);
        }
        if (columnsBy == null) {
            columnsBy = new HashMap<TransformColumn.IncludeOnType, LinkedHashMap<String, String>>(2);
        }
        LinkedHashMap<String, String> columnValues = columnsBy.get(column.getIncludeOn());
        if (columnValues == null) {
            columnValues = new LinkedHashMap<String, String>();
            columnsBy.put(column.getIncludeOn(), columnValues);
        }
        columnValues.put(column.getTargetColumnName(), columnValue);
    }

    protected List<String> retrieve(
            Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> source,
            boolean getColumnNames) {
        List<String> list = new ArrayList<String>(source == null ? 0 : source.size());
        if (source != null) {
            LinkedHashMap<String, String> values = source.get(IncludeOnType.ALL);
            if (values != null) {
                if (getColumnNames) {
                    list.addAll(values.keySet());
                } else {
                    list.addAll(values.values());
                }
            }

            IncludeOnType type = IncludeOnType.DELETE;
            if (targetDmlType == DataEventType.UPDATE && sourceDmlType != DataEventType.DELETE) {
                type = IncludeOnType.UPDATE;
            } else if (targetDmlType == DataEventType.INSERT) {
                type = IncludeOnType.INSERT;
            }

            values = source.get(type);
            if (values != null) {
                if (getColumnNames) {
                    list.addAll(values.keySet());
                } else {
                    list.addAll(values.values());
                }
            }
        }
        return list;
    }

    public String[] getKeyNames() {
        List<String> list = retrieve(keysBy, true);
        return list.toArray(new String[list.size()]);
    }

    public String[] getKeyValues() {
        List<String> list = retrieve(keysBy, false);
        return list.toArray(new String[list.size()]);
    }

    public String[] getColumnNames() {
        List<String> list = retrieve(columnsBy, true);
        return list.toArray(new String[list.size()]);
    }

    public String[] getColumnValues() {
        List<String> list = retrieve(columnsBy, false);
        return list.toArray(new String[list.size()]);
    }

    public DataEventType getSourceDmlType() {
        return sourceDmlType;
    }

    public TransformedData copy() {
        try {
            TransformedData clone = (TransformedData) this.clone();
            clone.columnsBy = copy(columnsBy);
            clone.keysBy = copy(keysBy);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public TransformTable getTransformation() {
        return transformation;
    }

    public void setGeneratedIdentityNeeded(boolean generatedIdentityNeeded) {
        this.generatedIdentityNeeded = generatedIdentityNeeded;
    }

    public boolean isGeneratedIdentityNeeded() {
        return generatedIdentityNeeded;
    }

    protected Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> copy(
            Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> toCopy) {
        Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> newMap = new HashMap<TransformColumn.IncludeOnType, LinkedHashMap<String, String>>(
                toCopy.size());
        for (TransformColumn.IncludeOnType key : toCopy.keySet()) {
            LinkedHashMap<String, String> value = toCopy.get(key);
            newMap.put(key, new LinkedHashMap<String, String>(value));
        }
        return newMap;
    }

    public Map<String, String> getSourceKeyValues() {
        return sourceKeyValues;
    }

    public Map<String, String> getOldSourceValues() {
        return oldSourceValues;
    }

    public Map<String, String> getSourceValues() {
        return sourceValues;
    }

    public boolean hasSameKeyValues(String[] otherKeyValues) {
        String[] keyValues = getKeyValues();
        if (otherKeyValues != null) {
            if (keyValues != null) {
                if (keyValues.length != otherKeyValues.length) {
                    return false;
                }
                for (int i = 0; i < otherKeyValues.length; i++) {
                    if (!keyValues[i].equals(otherKeyValues[i])) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        } else if (keyValues != null) {
            return false;
        }
        return true;
    }

    public Table buildTargetTable() {
        Table table = null;
        String[] columnNames = getColumnNames();
        String[] keyNames = getKeyNames();
        if (columnNames != null && columnNames.length > 0) {
            table = new Table(transformation.getTargetCatalogName(),
                    transformation.getTargetSchemaName(), transformation.getTargetTableName());
            for (String colName : columnNames) {
                Column col = new Column(colName);
                table.addColumn(col);
                if (keyNames != null) {
                    for (String keyName : keyNames) {
                        if (keyName.equals(colName)) {
                            col.setPrimaryKey(true);
                        }
                    }
                }
            }
        }
        return table;
    }

    public CsvData buildTargetCsvData() {
        CsvData data = new CsvData(this.targetDmlType);
        data.putParsedData(CsvData.OLD_DATA, getOldColumnValues());
        data.putParsedData(CsvData.ROW_DATA, getColumnValues());
        data.putParsedData(CsvData.PK_DATA, getKeyValues());
        data.putAttribute(getClass().getName(), this);
        return data;
    }

    public String[] getOldColumnValues() {
        List<String> names = retrieve(columnsBy, true);
        List<String> values = new ArrayList<String>();
        for( String name : names ) {
            if( oldSourceValues.containsKey(name) )
                values.add(oldSourceValues.get(name));
            else
                values.add(null);
        }
        return values.toArray(new String[values.size()]);
    }
}
