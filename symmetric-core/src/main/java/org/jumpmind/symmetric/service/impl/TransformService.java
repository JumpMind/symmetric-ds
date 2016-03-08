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
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.transform.AdditiveColumnTransform;
import org.jumpmind.symmetric.io.data.transform.BinaryLeftColumnTransform;
import org.jumpmind.symmetric.io.data.transform.BshColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ClarionDateTimeColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ColumnPolicy;
import org.jumpmind.symmetric.io.data.transform.ColumnsToRowsKeyColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ColumnsToRowsValueColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ConstantColumnTransform;
import org.jumpmind.symmetric.io.data.transform.CopyColumnTransform;
import org.jumpmind.symmetric.io.data.transform.CopyIfChangedColumnTransform;
import org.jumpmind.symmetric.io.data.transform.IColumnTransform;
import org.jumpmind.symmetric.io.data.transform.IdentityColumnTransform;
import org.jumpmind.symmetric.io.data.transform.JavaColumnTransform;
import org.jumpmind.symmetric.io.data.transform.LeftColumnTransform;
import org.jumpmind.symmetric.io.data.transform.LookupColumnTransform;
import org.jumpmind.symmetric.io.data.transform.MathColumnTransform;
import org.jumpmind.symmetric.io.data.transform.MultiplierColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ParameterColumnTransform;
import org.jumpmind.symmetric.io.data.transform.RemoveColumnTransform;
import org.jumpmind.symmetric.io.data.transform.SubstrColumnTransform;
import org.jumpmind.symmetric.io.data.transform.TargetDmlAction;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformColumn.IncludeOnType;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.transform.TrimColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ValueMapColumnTransform;
import org.jumpmind.symmetric.io.data.transform.VariableColumnTransform;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.util.FormatUtils;

public class TransformService extends AbstractService implements ITransformService {

    private Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> transformsCacheByNodeGroupLinkByTransformPoint;

    private long lastCacheTimeInMs;

    private IConfigurationService configurationService;
    
    private IExtensionService extensionService;
    
    private IParameterService parameterService;

    private Date lastUpdateTime;

    public TransformService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IConfigurationService configurationService, IExtensionService extensionService) {
        super(parameterService, symmetricDialect);
        this.configurationService = configurationService;
        this.extensionService = extensionService;
        this.parameterService = parameterService;
        
        addColumnTransform(ParameterColumnTransform.NAME, new ParameterColumnTransform(parameterService));
        addColumnTransform(VariableColumnTransform.NAME, new VariableColumnTransform());
        addColumnTransform(LookupColumnTransform.NAME, new LookupColumnTransform());
        addColumnTransform(BshColumnTransform.NAME, new BshColumnTransform(parameterService));
        addColumnTransform(AdditiveColumnTransform.NAME, new AdditiveColumnTransform());
        addColumnTransform(JavaColumnTransform.NAME, new JavaColumnTransform(extensionService));
        addColumnTransform(ConstantColumnTransform.NAME, new ConstantColumnTransform());
        addColumnTransform(CopyColumnTransform.NAME, new CopyColumnTransform());
        addColumnTransform(IdentityColumnTransform.NAME, new IdentityColumnTransform());
        addColumnTransform(MultiplierColumnTransform.NAME, new MultiplierColumnTransform());
        addColumnTransform(SubstrColumnTransform.NAME, new SubstrColumnTransform());
        addColumnTransform(LeftColumnTransform.NAME, new LeftColumnTransform());
        addColumnTransform(TrimColumnTransform.NAME, new TrimColumnTransform());
        addColumnTransform(BinaryLeftColumnTransform.NAME, new BinaryLeftColumnTransform());
        addColumnTransform(RemoveColumnTransform.NAME, new RemoveColumnTransform());
        addColumnTransform(MathColumnTransform.NAME, new MathColumnTransform());
        addColumnTransform(ValueMapColumnTransform.NAME, new ValueMapColumnTransform());
        addColumnTransform(CopyIfChangedColumnTransform.NAME, new CopyIfChangedColumnTransform());
        addColumnTransform(ColumnsToRowsKeyColumnTransform.NAME, new ColumnsToRowsKeyColumnTransform());
        addColumnTransform(ColumnsToRowsValueColumnTransform.NAME, new ColumnsToRowsValueColumnTransform());
        addColumnTransform(ClarionDateTimeColumnTransform.NAME, new ClarionDateTimeColumnTransform());
        
        setSqlMap(new TransformServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }    
    
    private void addColumnTransform(String name, IColumnTransform<?> columnTransform) {
        extensionService.addExtensionPoint(name, columnTransform);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, IColumnTransform<?>> getColumnTransforms() {
        return (Map) extensionService.getExtensionPointMap(IColumnTransform.class);
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

    public List<TransformTableNodeGroupLink> findTransformsFor(NodeGroupLink nodeGroupLink) {
        return findTransformsFor(nodeGroupLink, null);
    }
    
    public List<TransformTableNodeGroupLink> findTransformsFor(NodeGroupLink nodeGroupLink,
            TransformPoint transformPoint) {
        Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> byLinkByTransformPoint = 
                readInCacheIfExpired();
        Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = byLinkByTransformPoint
                .get(nodeGroupLink);
        if (byTransformPoint != null) {
            if (transformPoint != null) {
                return byTransformPoint.get(transformPoint);
            } else {
                // Transform point not specified, so return all transforms.
                List<TransformTableNodeGroupLink> transformsExtract = byTransformPoint
                        .get(TransformPoint.EXTRACT);
                List<TransformTableNodeGroupLink> transformsLoad = byTransformPoint
                        .get(TransformPoint.LOAD);
                List<TransformTableNodeGroupLink> transforms = new ArrayList<TransformTableNodeGroupLink>();
                if (transformsExtract != null) {
                    transforms.addAll(transformsExtract);
                }
                if (transformsLoad != null) {
                    transforms.addAll(transformsLoad);
                }
                return transforms;
            }
        }
        return null;
    }
    
    public List<TransformTableNodeGroupLink> findTransformsFor(String sourceNodeGroupId, String targetNodeGroupId, String table) {
        NodeGroupLink nodeGroupLink = new NodeGroupLink(sourceNodeGroupId, targetNodeGroupId);
        
        List<TransformTableNodeGroupLink> transformsForNodeGroupLink = findTransformsFor(nodeGroupLink);
        
        if (!CollectionUtils.isEmpty(transformsForNodeGroupLink)) {            
            List<TransformTableNodeGroupLink> transforms = new ArrayList<TransformTableNodeGroupLink>();
            
            for (TransformTableNodeGroupLink transform : transformsForNodeGroupLink) {
                if (StringUtils.equalsIgnoreCase(table, transform.getSourceTableName())) {
                    transforms.add(transform);
                }
            }
            
            if (!transforms.isEmpty()) {
                return transforms;
            } 
        }
        
        return null;
    }

    public void clearCache() {
        synchronized (this) {
            this.transformsCacheByNodeGroupLinkByTransformPoint = null;
        }
    }

    private Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> readInCacheIfExpired() {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRANSFORM_IN_MS);
        
        Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> byByLinkByTransformPoint = transformsCacheByNodeGroupLinkByTransformPoint;

        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || byByLinkByTransformPoint == null) {

                byByLinkByTransformPoint = new HashMap<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>>();

                List<TransformTableNodeGroupLink> transforms = getTransformTablesFromDB(true, true);

                for (TransformTableNodeGroupLink transformTable : transforms) {
                    NodeGroupLink nodeGroupLink = transformTable.getNodeGroupLink();
                    Map<TransformPoint, List<TransformTableNodeGroupLink>> byTransformPoint = byByLinkByTransformPoint
                            .get(nodeGroupLink);
                    if (byTransformPoint == null) {
                        byTransformPoint = new HashMap<TransformPoint, List<TransformTableNodeGroupLink>>();
                        byByLinkByTransformPoint.put(nodeGroupLink,
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
                this.transformsCacheByNodeGroupLinkByTransformPoint = byByLinkByTransformPoint;
            }
        }
        
        return byByLinkByTransformPoint;
    }

    private List<TransformTableNodeGroupLink> getTransformTablesFromDB(boolean includeColumns, boolean replaceTokens) {
        List<TransformTableNodeGroupLink> transforms = sqlTemplate.query(
                getSql("selectTransformTable"), new TransformTableMapper());
        if (includeColumns) {
            List<TransformColumn> columns = getTransformColumnsFromDB();
            for (TransformTableNodeGroupLink transformTable : transforms) {
                for (TransformColumn column : columns) {
                    if (column.getTransformId().equals(transformTable.getTransformId())) {
                        transformTable.addTransformColumn(column);
                    }

                }
            }
        }
        if (replaceTokens) {
    		@SuppressWarnings({ "rawtypes", "unchecked" })
			Map<String, String> replacements = (Map) parameterService.getAllParameters();
        	for (TransformTableNodeGroupLink transform : transforms) {
        		transform.setSourceCatalogName(FormatUtils.replaceTokens(transform.getSourceCatalogName(), replacements, true));
        		transform.setSourceSchemaName(FormatUtils.replaceTokens(transform.getSourceSchemaName(), replacements, true));
        		transform.setSourceTableName(FormatUtils.replaceTokens(transform.getSourceTableName(), replacements, true));
        		transform.setTargetCatalogName(FormatUtils.replaceTokens(transform.getTargetCatalogName(), replacements, true));
        		transform.setTargetSchemaName(FormatUtils.replaceTokens(transform.getTargetSchemaName(), replacements, true));
        		transform.setTargetTableName(FormatUtils.replaceTokens(transform.getTargetTableName(), replacements, true));
        	}
        }
        return transforms;
    }

    private List<TransformColumn> getTransformColumnsFromDB() {
        List<TransformColumn> columns = sqlTemplate.query(getSql("selectTransformColumn"),
                new TransformColumnMapper());
        return columns;
    }

    public List<TransformTableNodeGroupLink> getTransformTables(boolean includeColumns) {
        return this.getTransformTablesFromDB(includeColumns, true);
    }

    public List<TransformTableNodeGroupLink> getTransformTables(boolean includeColumns, boolean replaceTokens) {
        return this.getTransformTablesFromDB(includeColumns, replaceTokens);
    }

    public List<TransformColumn> getTransformColumns() {
        return this.getTransformColumnsFromDB();
    }

    @Override
    public List<TransformColumn> getTransformColumnsForTable(String transformId) {
        List<TransformColumn> columns = sqlTemplate.query(getSql("selectTransformColumnForTable"),
                new TransformColumnMapper(), transformId);
        return columns;
    }

    public void saveTransformTable(TransformTableNodeGroupLink transformTable, boolean saveTransformColumns) {
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
                            .toString(), transformTable.getUpdateAction(), transformTable.getTransformOrder(), transformTable
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
                                .toString(), transformTable.getUpdateAction(), transformTable.getTransformOrder(), transformTable
                                .getColumnPolicy().toString(), transformTable.getLastUpdateTime(),
                        transformTable.getLastUpdateBy(), transformTable.getCreateTime(),
                        transformTable.getTransformId());
            }
            
            if (saveTransformColumns) {
                deleteTransformColumns(transaction, transformTable.getTransformId());
                List<TransformColumn> columns = transformTable.getTransformColumns();
                if (columns != null) {
                    for (TransformColumn transformColumn : columns) {
                        saveTransformColumn(transaction, transformColumn);
                    }
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
            clearCache();
        }        
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
            clearCache();
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
            table.setNodeGroupLink(configurationService
               .getNodeGroupLinkFor(rs.getString("source_node_group_id"), rs.getString("target_node_group_id"), false));
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
            table.setUpdateAction(rs.getString("update_action"));
            table.setDeleteAction(TargetDmlAction.valueOf(rs.getString("delete_action")));
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
