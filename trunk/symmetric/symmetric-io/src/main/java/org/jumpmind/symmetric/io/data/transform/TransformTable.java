package org.jumpmind.symmetric.io.data.transform;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.model.Table;

public class TransformTable {

    protected String transformId;
    protected String sourceCatalogName;
    protected String sourceSchemaName;
    protected String sourceTableName;
    protected String targetCatalogName;
    protected String targetSchemaName;
    protected String targetTableName;
    protected TransformPoint transformPoint;
    protected List<TransformColumn> transformColumns;
    protected List<TransformColumn> primaryKeyColumns;
    protected DeleteAction deleteAction = DeleteAction.NONE;
    protected boolean updateFirst = false;
    protected int transformOrder = 0;

    public TransformTable(String sourceTableName, String targetTableName,
            TransformPoint transformPoint, TransformColumn... columns) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.transformPoint = transformPoint;
        this.transformColumns = new ArrayList<TransformColumn>();
        this.primaryKeyColumns = new ArrayList<TransformColumn>();
        if (columns != null) {
            for (TransformColumn transformColumn : columns) {
                if (transformColumn.isPk()) {
                    primaryKeyColumns.add(transformColumn);
                }
                transformColumns.add(transformColumn);
            }
        }
    }

    public TransformTable() {

    }

    public String getFullyQualifiedSourceTableName() {
        return Table.getFullyQualifiedTableName(sourceCatalogName, sourceSchemaName,
                sourceTableName);
    }

    public String getFullyQualifiedTargetTableName() {
        return Table.getFullyQualifiedTableName(sourceCatalogName, sourceSchemaName,
                sourceTableName);
    }

    public String getTransformId() {
        return transformId;
    }

    public void setTransformId(String transformId) {
        this.transformId = transformId;
    }

    public String getSourceCatalogName() {
        return sourceCatalogName;
    }

    public void setSourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String sourceSchemaName) {
        this.sourceSchemaName = sourceSchemaName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetCatalogName() {
        return targetCatalogName;
    }

    public void setTargetCatalogName(String targetCatalogName) {
        this.targetCatalogName = targetCatalogName;
    }

    public String getTargetSchemaName() {
        return targetSchemaName;
    }

    public void setTargetSchemaName(String targetSchemaName) {
        this.targetSchemaName = targetSchemaName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public void setTransformPoint(TransformPoint transformPoint) {
        this.transformPoint = transformPoint;
    }

    public TransformPoint getTransformPoint() {
        return transformPoint;
    }

    public void setTransformColumns(List<TransformColumn> transformColumns) {
        this.transformColumns = transformColumns;
        this.primaryKeyColumns = new ArrayList<TransformColumn>();
        if (transformColumns != null) {
            for (TransformColumn transformColumn : transformColumns) {
                if (transformColumn.isPk()) {
                    this.primaryKeyColumns.add(transformColumn);
                }
            }
        }
    }

    public List<TransformColumn> getTransformColumns() {
        return transformColumns;
    }

    public List<TransformColumn> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public List<TransformColumn> getTransformColumnFor(String columnName) {
        List<TransformColumn> columns = new ArrayList<TransformColumn>(2);
        for (TransformColumn column : transformColumns) {
            if (column.getSourceColumnName().equals(columnName)) {
                columns.add(column);
            }
        }
        return columns;
    }

    public void addTransformColumn(TransformColumn column) {
        if (transformColumns == null) {
            transformColumns = new ArrayList<TransformColumn>();
        }
        if (primaryKeyColumns == null) {
            primaryKeyColumns = new ArrayList<TransformColumn>();
        }
        transformColumns.add(column);
        if (column.isPk()) {
            primaryKeyColumns.add(column);
        }
    }

    public void setDeleteAction(DeleteAction deleteAction) {
        this.deleteAction = deleteAction;
    }

    public DeleteAction getDeleteAction() {
        return deleteAction;
    }

    public void setTransformOrder(int transformOrder) {
        this.transformOrder = transformOrder;
    }

    public int getTransformOrder() {
        return transformOrder;
    }

    public void setUpdateFirst(boolean updateFirst) {
        this.updateFirst = updateFirst;
    }

    public boolean isUpdateFirst() {
        return updateFirst;
    }

    @Override
    public int hashCode() {
        if (transformId != null) {
            return transformId.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (transformId != null) {
            if (obj instanceof TransformTable) {
                return transformId.equals(((TransformTable) obj).transformId);
            } else {
                return false;
            }
        } else {
            return super.equals(obj);
        }
    }

    @Override
    public String toString() {
        if (transformId != null) {
            return transformId;
        } else {
            return super.toString();
        }
    }
}
