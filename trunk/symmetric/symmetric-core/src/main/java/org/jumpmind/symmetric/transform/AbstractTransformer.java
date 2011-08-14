package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IParameterService;

public abstract class AbstractTransformer {

    protected final ILog log = LogFactory.getLog(getClass());

    protected ITransformService transformService;

    protected IDbDialect dbDialect;

    protected IParameterService parameterService;

    protected String tablePrefix;
    
    protected boolean isEligibleForTransform(String catalogName, String schemaName, String tableName) {
        return !tableName.toLowerCase().startsWith(tablePrefix);
    }

    protected List<TransformedData> transform(DmlType dmlType, ICacheContext context,
            NodeGroupLink nodeGroupLink, String catalogName, String schemaName, String tableName,
            String[] columnNames, String[] columnValues, String[] keyNames, String[] keyValues,
            String[] oldData) throws IgnoreRowException {
        if (isEligibleForTransform(catalogName, schemaName, tableName)) {
            List<TransformTable> transformationsToPerform = findTablesToTransform(nodeGroupLink,
                    getFullyQualifiedTableName(catalogName, schemaName, tableName));
            if (transformationsToPerform != null && transformationsToPerform.size() > 0) {
                Map<String, String> sourceValues = toMap(columnNames, columnValues);
                Map<String, String> oldSourceValues = toMap(columnNames, oldData);
                Map<String, String> sourceKeyValues = sourceValues;
                if (keyNames != null) {
                    sourceKeyValues = toMap(keyNames, keyValues);
                }
                List<TransformedData> dataThatHasBeenTransformed = new ArrayList<TransformedData>();
                for (TransformTable transformation : transformationsToPerform) {
                    List<TransformedData> dataToTransform = create(context, dmlType,
                            transformation, sourceKeyValues, oldSourceValues);
                    for (TransformedData targetData : dataToTransform) {
                        if (perform(context, targetData, transformation, sourceValues,
                                oldSourceValues)) {
                            dataThatHasBeenTransformed.add(targetData);
                        }
                    }
                }
                return dataThatHasBeenTransformed;
            }
        }
        return null;
    }

    protected boolean perform(ICacheContext context, TransformedData data,
            TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        boolean persistData = false;

        for (TransformColumn transformColumn : transformation.getTransformColumns()) {
            if (transformColumn.getSourceColumnName() == null
                    || sourceValues.containsKey(transformColumn.getSourceColumnName())) {
                IColumnTransform<?> transform = transformService.getColumnTransforms().get(
                        transformColumn.getTransformType());
                if (transform == null || transform instanceof ISingleValueColumnTransform) {
                    try {
                        String value = (String) transformColumn(context, data, transformColumn,
                                sourceValues, oldSourceValues);
                        data.put(transformColumn, value, false);
                    } catch (IgnoreColumnException e) {
                        // Do nothing. We are ignoring the column
                    }
                }
            } else {
                log.warn("TransformSourceColumnNotFound", transformColumn.getSourceColumnName(),
                        transformation.getTransformId());
            }
        }

        if (data.getTargetDmlType() != DmlType.DELETE) {
            if (data.getTargetDmlType() == DmlType.INSERT && transformation.isUpdateFirst()) {
                data.setTargetDmlType(DmlType.UPDATE);
            }
            persistData = true;
        } else {
            // handle the delete action
            DeleteAction deleteAction = transformation.getDeleteAction();
            switch (deleteAction) {
            case NONE:
                break;
            case DEL_ROW:
                data.setTargetDmlType(DmlType.DELETE);
                persistData = true;
                break;
            case UPDATE_COL:
                data.setTargetDmlType(DmlType.UPDATE);
                persistData = true;
                break;
            }
        }
        return persistData;
    }

    protected List<TransformedData> create(ICacheContext context, DmlType dmlType,
            TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        List<TransformedData> datas = new ArrayList<TransformedData>();
        datas.add(new TransformedData(transformation, dmlType));
        List<TransformColumn> columns = transformation.getPrimaryKeyColumns();
        if (columns.size() == 0) {
            log.error("TransformNoPrimaryKeyDefined", transformation.getTransformId());
        } else {
            for (TransformColumn transformColumn : columns) {
                List<TransformedData> newDatas = null;
                for (TransformedData data : datas) {
                    try {
                        Object columnValue = transformColumn(context, data, transformColumn,
                                sourceValues, oldSourceValues);
                        if (columnValue instanceof String) {
                            data.put(transformColumn, (String) columnValue, true);
                        } else if (columnValue instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<String> values = (List<String>) columnValue;
                            if (values.size() > 0) {
                                data.put(transformColumn, values.get(0), true);
                                if (values.size() > 1) {
                                    if (newDatas == null) {
                                        newDatas = new ArrayList<TransformedData>(values.size() - 1);
                                    }
                                    for (int i = 1; i < values.size(); i++) {
                                        TransformedData newData = data.copy();
                                        newData.put(transformColumn, values.get(i), true);
                                        newDatas.add(newData);
                                    }
                                }
                            } else {
                                throw new IgnoreRowException();
                            }
                        }
                    } catch (IgnoreColumnException e) {
                        // Do nothing. We are suppose to ignore the column.
                    }
                }
                if (newDatas != null) {
                    datas.addAll(newDatas);
                    newDatas = null;
                }
            }
        }
        return datas;
    }

    protected Object transformColumn(ICacheContext context, TransformedData data,
            TransformColumn transformColumn, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException, IgnoreColumnException {
        Object returnValue = null;
        String value = transformColumn.getSourceColumnName() != null ? sourceValues
                .get(transformColumn.getSourceColumnName()) : null;
        returnValue = value;
        IColumnTransform<?> transform = transformService.getColumnTransforms().get(
                transformColumn.getTransformType());
        if (transform != null) {
            String oldValue = oldSourceValues.get(transformColumn.getSourceColumnName());
            returnValue = transform.transform(context, transformColumn, data, sourceValues, value,
                    oldValue);
        }
        return returnValue;
    }

    protected String getFullyQualifiedTableName(String catalogName, String schemaName,
            String tableName) {
        if (!StringUtils.isBlank(schemaName)) {
            tableName = schemaName + "." + tableName;
        }
        if (!StringUtils.isBlank(catalogName)) {
            tableName = catalogName + "." + tableName;
        }
        return tableName;
    }

    abstract protected TransformPoint getTransformPoint();

    protected List<TransformTable> findTablesToTransform(NodeGroupLink nodeGroupLink,
            String fullyQualifiedSourceTableName) {
        Map<String, List<TransformTable>> transformMap = transformService.findTransformsFor(
                nodeGroupLink, getTransformPoint(), true);
        List<TransformTable> transforms = transformMap != null ? transformMap
                .get(fullyQualifiedSourceTableName) : null;
        return transforms;
    }

    protected Map<String, String> toMap(String[] columnNames, String[] columnValues) {
        if (columnValues != null) {
            Map<String, String> map = new HashMap<String, String>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                map.put(columnNames[i], columnValues[i]);
            }
            return map;
        } else {
            return new HashMap<String, String>(0);
        }
    }

    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public boolean isAutoRegister() {
        return true;
    }

}
