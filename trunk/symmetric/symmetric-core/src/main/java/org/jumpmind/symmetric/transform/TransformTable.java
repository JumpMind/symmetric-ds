package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ddl.model.Table;

public class TransformTable {

    protected String transformId;
    protected String sourceCatalogName;
    protected String sourceSchemaName;
    protected String sourceTableName;
    protected String targetCatalogName;
    protected String targetSchemaName;
    protected String targetTableName;
    protected String targetNodeGroupId;
    protected List<TransformColumn> transformColumns;
    protected List<TransformColumn> primaryKeyColumns;
    protected DeleteAction deleteAction = DeleteAction.NONE;
    protected boolean updateFirst = false;
    protected int transformOrder = 0;

    public String getFullyQualifiedSourceTableName() {
        return Table.getFullyQualifiedTableName(sourceTableName, sourceSchemaName,
                sourceCatalogName);
    }
    
    public String getFullyQualifiedTargetTableName() {
        return Table.getFullyQualifiedTableName(targetTableName, targetSchemaName,
                targetCatalogName);
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

    public String getTargetNodeGroupId() {
        return targetNodeGroupId;
    }

    public void setTargetNodeGroupId(String targetNodeGroupId) {
        this.targetNodeGroupId = targetNodeGroupId;
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
}
