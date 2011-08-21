package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.transform.TransformColumn.IncludeOnType;

public class TransformedData implements Cloneable {

    protected DmlType targetDmlType;

    protected DmlType sourceDmlType;

    protected Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> columnsBy;

    protected Map<TransformColumn.IncludeOnType, LinkedHashMap<String, String>> keysBy;

    protected TransformTable transformation;

    public TransformedData(TransformTable transformation, DmlType sourceDmlType) {
        this.transformation = transformation;
        this.targetDmlType = sourceDmlType;
        this.sourceDmlType = sourceDmlType;
    }

    public String getFullyQualifiedTableName() {
        return transformation.getFullyQualifiedTargetTableName();
    }

    public DmlType getTargetDmlType() {
        return targetDmlType;
    }

    public void setTargetDmlType(DmlType dmlType) {
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
            if (targetDmlType == DmlType.UPDATE && sourceDmlType != DmlType.DELETE) {
                type = IncludeOnType.UPDATE;
            } else if (targetDmlType == DmlType.INSERT) {
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
        if (keysBy == null) {
            throw new RuntimeException("No primary key defined for " + transformation.getTransformId());
        }
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

    public DmlType getSourceDmlType() {
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

}
