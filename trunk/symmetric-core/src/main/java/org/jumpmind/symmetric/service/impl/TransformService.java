package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.transform.ColumnPolicy;
import org.jumpmind.symmetric.io.data.transform.DeleteAction;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformColumn.IncludeOnType;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;

public class TransformService extends AbstractService implements ITransformService {

    private Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> transformsCacheByNodeGroupLinkByTransformPoint;

    private long lastCacheTimeInMs;

    private IConfigurationService configurationService;
    
    private Date lastUpdateTime;

    public TransformService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IConfigurationService configurationService) {
        super(parameterService, symmetricDialect);
        this.configurationService = configurationService;
        setSqlMap(new TransformServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }
    
    public boolean refreshFromDatabase() {
        Date date1 = sqlTemplate.queryForObject(getSql("selectMaxTransformTableLastUpdateTime"), Date.class);
        Date date2 = sqlTemplate.queryForObject(getSql("selectMaxTransformColumnLastUpdateTime"), Date.class);
        Date date = maxDate(date1, date2);
        
        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                   log.info("Newer transform settings were detected");
                }
                lastUpdateTime = date;
                clearCache();
                return true;
            }
        }
        return false;
    }


    public List<TransformTableNodeGroupLink> findTransformsFor(NodeGroupLink nodeGroupLink,
            TransformPoint transformPoint, boolean useCache) {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);

        // if the cache is expired or the caller doesn't want to use the cache,
        // pull the data and refresh the cache
        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || transformsCacheByNodeGroupLinkByTransformPoint == null || useCache == false) {
                refreshCache();
            }
        }

        if (transformsCacheByNodeGroupLinkByTransformPoint != null) {
            Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = transformsCacheByNodeGroupLinkByTransformPoint
                    .get(nodeGroupLink);
            if (byTransformPoint != null) {
                return byTransformPoint.get(transformPoint);
            }
        }
        return null;
    }

    public void clearCache() {
        synchronized (this) {
            this.transformsCacheByNodeGroupLinkByTransformPoint = null;
        }
    }

    protected void refreshCache() {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);

        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || transformsCacheByNodeGroupLinkByTransformPoint == null) {

                transformsCacheByNodeGroupLinkByTransformPoint = new HashMap<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>>();

                List<TransformTableNodeGroupLink> transforms = getTransformTablesFromDB();

                for (TransformTableNodeGroupLink transformTable : transforms) {
                    NodeGroupLink nodeGroupLink = transformTable.getNodeGroupLink();
                    Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = transformsCacheByNodeGroupLinkByTransformPoint
                            .get(nodeGroupLink);
                    if (byTransformPoint == null) {
                        byTransformPoint = new HashMap<TransformPoint, List<TransformTableNodeGroupLink>>();
                        transformsCacheByNodeGroupLinkByTransformPoint.put(nodeGroupLink,
                                byTransformPoint);
                    }

                    List<TransformTableNodeGroupLink> byTableName = byTransformPoint
                            .get(transformTable.getTransformPoint());
                    if (byTableName == null) {
                        byTableName = new ArrayList<TransformTableNodeGroupLink>();
                        byTransformPoint.put(transformTable.getTransformPoint(), byTableName);
                    }

                    byTableName.add(transformTable);
                }
                lastCacheTimeInMs = System.currentTimeMillis();
            }
        }
    }

    private List<TransformTableNodeGroupLink> getTransformTablesFromDB() {
        List<TransformTableNodeGroupLink> transforms = sqlTemplate.query(
                getSql("selectTransformTable"), new TransformTableMapper());
        List<TransformColumn> columns = getTransformColumnsFromDB();
        for (TransformTableNodeGroupLink transformTable : transforms) {
            for (TransformColumn column : columns) {
                if (column.getTransformId().equals(transformTable.getTransformId())) {
                    transformTable.addTransformColumn(column);
                }

            }
        }
        return transforms;
    }

    private List<TransformColumn> getTransformColumnsFromDB() {
        List<TransformColumn> columns = sqlTemplate.query(getSql("selectTransformColumn"),
                new TransformColumnMapper());
        return columns;
    }

    public List<TransformTableNodeGroupLink> getTransformTables() {
        return this.getTransformTablesFromDB();
    }

    public List<TransformColumn> getTransformColumns() {
        return this.getTransformColumnsFromDB();
    }

    public List<TransformColumn> getTransformColumnsForTable() {
        List<TransformColumn> columns = sqlTemplate.query(getSql("selectTransformColumnForTable"),
                new TransformColumnMapper());
        return columns;
    }

    public void saveTransformTable(TransformTableNodeGroupLink transformTable) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            transformTable.setLastUpdateTime(new Date());
            if (transaction.prepareAndExecute(getSql("updateTransformTableSql"), transformTable
                    .getNodeGroupLink().getSourceNodeGroupId(), transformTable.getNodeGroupLink()
                    .getTargetNodeGroupId(), transformTable.getSourceCatalogName(), transformTable
                    .getSourceSchemaName(), transformTable.getSourceTableName(), transformTable
                    .getTargetCatalogName(), transformTable.getTargetSchemaName(), transformTable
                    .getTargetTableName(), transformTable.getTransformPoint().toString(),
                    transformTable.isUpdateFirst() ? 1 : 0, transformTable.getDeleteAction()
                            .toString(), transformTable.getTransformOrder(), transformTable
                            .getColumnPolicy().toString(), transformTable.getLastUpdateTime(),
                    transformTable.getLastUpdateBy(), transformTable.getTransformId()) == 0) {
                transformTable.setCreateTime(new Date());
                transaction.prepareAndExecute(getSql("insertTransformTableSql"), transformTable
                        .getNodeGroupLink().getSourceNodeGroupId(), transformTable
                        .getNodeGroupLink().getTargetNodeGroupId(), transformTable
                        .getSourceCatalogName(), transformTable.getSourceSchemaName(),
                        transformTable.getSourceTableName(), transformTable.getTargetCatalogName(),
                        transformTable.getTargetSchemaName(), transformTable.getTargetTableName(),
                        transformTable.getTransformPoint().toString(), transformTable
                                .isUpdateFirst() ? 1 : 0, transformTable.getDeleteAction()
                                .toString(), transformTable.getTransformOrder(), transformTable
                                .getColumnPolicy().toString(), transformTable.getLastUpdateTime(),
                        transformTable.getLastUpdateBy(), transformTable.getCreateTime(),
                        transformTable.getTransformId());
            }
            deleteTransformColumns(transaction, transformTable.getTransformId());
            List<TransformColumn> columns = transformTable.getTransformColumns();
            if (columns != null) {
                for (TransformColumn transformColumn : columns) {
                    saveTransformColumn(transaction, transformColumn);
                }
            }
            transaction.commit();
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;              
        } finally {
            close(transaction);
        }
        refreshCache();
    }

    protected void deleteTransformColumns(ISqlTransaction transaction, String transformTableId) {
        transaction.prepareAndExecute(getSql("deleteTransformColumnsSql"),
                (Object) transformTableId);
    }

    public void deleteTransformTable(String transformTableId) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            deleteTransformColumns(transaction, transformTableId);
            transaction.prepareAndExecute(getSql("deleteTransformTableSql"),
                    (Object) transformTableId);
            transaction.commit();
            refreshCache();
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;              
        } finally {
            close(transaction);
        }
    }

    protected void saveTransformColumn(ISqlTransaction transaction, TransformColumn transformColumn) {
        transformColumn.setLastUpdateTime(new Date());
        if (transaction.prepareAndExecute(getSql("updateTransformColumnSql"),
                transformColumn.getSourceColumnName(), transformColumn.isPk() ? 1 : 0,
                transformColumn.getTransformType(), transformColumn.getTransformExpression(),
                transformColumn.getTransformOrder(), transformColumn.getLastUpdateTime(),
                transformColumn.getLastUpdateBy(), transformColumn.getTransformId(),
                transformColumn.getIncludeOn().toDbValue(), transformColumn.getTargetColumnName()) == 0) {
            transformColumn.setCreateTime(new Date());
            transaction.prepareAndExecute(getSql("insertTransformColumnSql"),
                    transformColumn.getTransformId(), transformColumn.getIncludeOn().toDbValue(),
                    transformColumn.getTargetColumnName(), transformColumn.getSourceColumnName(),
                    transformColumn.isPk() ? 1 : 0, transformColumn.getTransformType(),
                    transformColumn.getTransformExpression(), transformColumn.getTransformOrder(),
                    transformColumn.getLastUpdateTime(), transformColumn.getLastUpdateBy(),
                    transformColumn.getCreateTime());
        }
    }

    class TransformTableMapper implements ISqlRowMapper<TransformTableNodeGroupLink> {

        public TransformTableNodeGroupLink mapRow(Row rs) {
            TransformTableNodeGroupLink table = new TransformTableNodeGroupLink();
            table.setTransformId(rs.getString("transform_id"));
            table.setNodeGroupLink(configurationService.getNodeGroupLinkFor(
                    rs.getString("source_node_group_id"), rs.getString("target_node_group_id")));
            table.setSourceCatalogName(rs.getString("source_catalog_name"));
            table.setSourceSchemaName(rs.getString("source_schema_name"));
            table.setSourceTableName(rs.getString("source_table_name"));
            table.setTargetCatalogName(rs.getString("target_catalog_name"));
            table.setTargetSchemaName(rs.getString("target_schema_name"));
            table.setTargetTableName(rs.getString("target_table_name"));
            try {
                table.setTransformPoint(TransformPoint.valueOf(rs.getString("transform_point")
                        .toUpperCase()));
            } catch (RuntimeException ex) {
                log.warn(
                        "Invalid value provided for transform_point of '{}.'  Valid values are: {}",
                        rs.getString("transform_point"), Arrays.toString(TransformPoint.values()));
                throw ex;
            }
            table.setTransformOrder(rs.getInt("transform_order"));
            table.setUpdateFirst(rs.getBoolean("update_first"));
            table.setColumnPolicy(ColumnPolicy.valueOf(rs.getString("column_policy")));
            table.setDeleteAction(DeleteAction.valueOf(rs.getString("delete_action")));
            table.setCreateTime(rs.getDateTime("create_time"));
            table.setLastUpdateBy(rs.getString("last_update_by"));
            table.setLastUpdateTime(rs.getDateTime("last_update_time"));
            return table;
        }
    }

    class TransformColumnMapper implements ISqlRowMapper<TransformColumn> {
        public TransformColumn mapRow(Row rs) {
            TransformColumn col = new TransformColumn();
            col.setTransformId(rs.getString("transform_id"));
            col.setIncludeOn(IncludeOnType.decode(rs.getString("include_on")));
            col.setTargetColumnName(rs.getString("target_column_name"));
            col.setSourceColumnName(rs.getString("source_column_name"));
            col.setPk(rs.getBoolean("pk"));
            col.setTransformType(rs.getString("transform_type"));
            col.setTransformExpression(rs.getString("transform_expression"));
            col.setTransformOrder(rs.getInt("transform_order"));
            col.setCreateTime(rs.getDateTime("create_time"));
            col.setLastUpdateBy(rs.getString("last_update_by"));
            col.setLastUpdateTime(rs.getDateTime("last_update_time"));
            return col;
        }
    }

    public static class TransformTableNodeGroupLink extends TransformTable {

        protected NodeGroupLink nodeGroupLink;

        public void setNodeGroupLink(NodeGroupLink nodeGroupLink) {
            this.nodeGroupLink = nodeGroupLink;
        }

        public NodeGroupLink getNodeGroupLink() {
            return nodeGroupLink;
        }
    }
}
