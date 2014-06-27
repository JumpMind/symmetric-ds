package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transform.IgnoreColumnException;
import org.jumpmind.symmetric.transform.IgnoreRowException;
import org.jumpmind.symmetric.transform.TransformColumn;
import org.jumpmind.symmetric.transform.TransformTable;
import org.jumpmind.symmetric.transform.TransformedData;
import org.jumpmind.symmetric.util.AppUtils;

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
            String[] oldData) {
        if (isEligibleForTransform(catalogName, schemaName, tableName)) {
            String fullyQualifiedName = getFullyQualifiedTableName(catalogName, schemaName, tableName);
            List<TransformTable> transformationsToPerform = findTablesToTransform(nodeGroupLink,
                    fullyQualifiedName);
            if (transformationsToPerform != null && transformationsToPerform.size() > 0) {
                Map<String, String> sourceValues = AppUtils.toMap(columnNames, columnValues);
                Map<String, String> oldSourceValues = AppUtils.toMap(columnNames, oldData);
                Map<String, String> sourceKeyValues = oldSourceValues.size() > 0 ? oldSourceValues
                        : sourceValues;
                if (keyNames != null && oldSourceValues.size() == 0) {
                    sourceKeyValues = AppUtils.toMap(keyNames, keyValues);
                }
                
                if (dmlType == DmlType.DELETE) {
                    sourceValues = oldSourceValues;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("TransformStarted", transformationsToPerform.size(), 
                            dmlType.toString(), fullyQualifiedName, sourceValues);
                }
                
                List<TransformedData> dataThatHasBeenTransformed = new ArrayList<TransformedData>();
                for (TransformTable transformation : transformationsToPerform) {
                    dataThatHasBeenTransformed.addAll(transform(dmlType, context, transformation,
                            sourceKeyValues, oldSourceValues, sourceValues));
                }
                return dataThatHasBeenTransformed;
            }
        }
        return null;
    }
    
    protected List<TransformedData> transform(DmlType dmlType, ICacheContext context,
            TransformTable transformation, Map<String, String> sourceKeyValues,
            Map<String, String> oldSourceValues, Map<String, String> sourceValues) {
        try {
            List<TransformedData> dataToTransform = create(context, dmlType, transformation,
                    sourceKeyValues, oldSourceValues, sourceValues);
            List<TransformedData> dataThatHasBeenTransformed = new ArrayList<TransformedData>(dataToTransform.size());
            if (log.isDebugEnabled()) {
                log.debug("TransformDataCreated", dataToTransform.size(),
                        transformation.getTransformId(),
                        transformation.getFullyQualifiedTargetTableName());
            }
            int transformNumber = 0;
            for (TransformedData targetData : dataToTransform) {
            	transformNumber++;
                if (perform(context, targetData, transformation, sourceValues, oldSourceValues)) {
                    if (log.isDebugEnabled()) {
                        log.debug("TransformedDataReadyForApplication", targetData
                                .getTargetDmlType().toString(), transformNumber, ArrayUtils.toString(targetData
                                .getColumnNames()), ArrayUtils.toString(targetData
                                .getColumnValues()));
                    }
                    dataThatHasBeenTransformed.add(targetData);
                } else {
                    log.debug("TransformNotPerformed", transformNumber);
                }
            }
            return dataThatHasBeenTransformed;
        } catch (IgnoreRowException ex) {
            // ignore this row
            if (log.isDebugEnabled()) {
                log.debug("TransformRowIgnored",
                        "transformation aborted during tranformation of key");
            }
            return new ArrayList<TransformedData>(0);
        }

    }

    protected boolean perform(ICacheContext context, TransformedData data,
            TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        boolean persistData = false;
        try {
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
                    log.warn("TransformSourceColumnNotFound",
                            transformColumn.getSourceColumnName(), transformation.getTransformId());
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
                case DEL_ROW:
                    data.setTargetDmlType(DmlType.DELETE);
                    persistData = true;
                    break;
                case UPDATE_COL:
                    data.setTargetDmlType(DmlType.UPDATE);
                    persistData = true;
                    break;                    
                case NONE:
                default:
                	if (log.isDebugEnabled()) {
                		log.debug("TransformNoActionNotConfiguredToDelete", transformation.getTransformId());
                	}
                    break;

                }
            }
        } catch (IgnoreRowException ex) {
            // ignore this row
            if (log.isDebugEnabled()) {
                log.debug("TransformRowIgnored", ArrayUtils.toString(data.getKeyValues()));
            }
        }
        return persistData;
    }

    protected List<TransformedData> create(ICacheContext context, DmlType dmlType,
            TransformTable transformation, Map<String, String> sourceKeyValues,
            Map<String, String> oldSourceValues, Map<String, String> sourceValues)
            throws IgnoreRowException {
        List<TransformColumn> columns = transformation.getPrimaryKeyColumns();
        if (columns == null || columns.size() == 0) {
            log.error("TransformNoPrimaryKeyDefined", transformation.getTransformId());
            return new ArrayList<TransformedData>(0);
        } else {
            ArrayList<TransformedData> datas = new ArrayList<TransformedData>();
            TransformedData data = new TransformedData(transformation, dmlType, sourceKeyValues,
                    oldSourceValues, sourceValues);
            for (TransformColumn transformColumn : columns) {
                List<TransformedData> newDatas = null;
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

                if (newDatas != null && newDatas.size() > 0) {
                    datas.addAll(newDatas);
                    newDatas = null;
                }
            }

            if (data.getColumnValues() != null && data.getColumnValues().length > 0) {
                datas.add(0, data);
            }

            return datas;

        }
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
