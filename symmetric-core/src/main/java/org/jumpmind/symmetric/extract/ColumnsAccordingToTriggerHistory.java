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
package org.jumpmind.symmetric.extract;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.transform.ColumnPolicy;
import org.jumpmind.symmetric.io.data.transform.RemoveColumnTransform;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.symmetric.util.SymmetricUtils;

public class ColumnsAccordingToTriggerHistory {
    private Map<CacheKey, Table> cache = new HashMap<CacheKey, Table>();
    private Node sourceNode;
    private Node targetNode;
    private ITriggerRouterService triggerRouterService;
    private ITransformService transformService;
    private ISymmetricDialect symmetricDialect;
    private String tablePrefix;

    public ColumnsAccordingToTriggerHistory(ISymmetricEngine engine, Node sourceNode, Node targetNode) {
        triggerRouterService = engine.getTriggerRouterService();
        transformService = engine.getTransformService();
        symmetricDialect = engine.getSymmetricDialect();
        tablePrefix = engine.getTablePrefix().toLowerCase();
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
    }

    public Table lookup(String routerId, TriggerHistory triggerHistory, boolean setTargetTableName,
            boolean useDatabaseDefinition, boolean useTransforms) {
        CacheKey key = new CacheKey(routerId, triggerHistory.getTriggerHistoryId(), setTargetTableName,
                useDatabaseDefinition, useTransforms);
        Table table = cache.get(key);
        if (table == null) {
            table = lookupAndOrderColumnsAccordingToTriggerHistory(routerId, triggerHistory, setTargetTableName,
                    useDatabaseDefinition, useTransforms);
            cache.put(key, table);
        }
        return table;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId, TriggerHistory triggerHistory,
            boolean setTargetTableName, boolean useDatabaseDefinition, boolean useTransforms) {
        String catalogName = triggerHistory.getSourceCatalogName();
        String schemaName = triggerHistory.getSourceSchemaName();
        String tableName = triggerHistory.getSourceTableName();
        String tableNameLowerCase = triggerHistory.getSourceTableNameLowerCase();
        Table table = null;
        if (useDatabaseDefinition) {
            table = getTargetPlatform(tableNameLowerCase).getTableFromCache(catalogName, schemaName, tableName, false);
            if (table != null && table.getColumnCount() < triggerHistory.getParsedColumnNames().length) {
                /*
                 * If the column count is less than what trigger history reports, then chances are the table cache is out of date.
                 */
                table = getTargetPlatform(tableNameLowerCase).getTableFromCache(catalogName, schemaName, tableName, true);
            }
            if (table != null) {
                table = table.copyAndFilterColumns(triggerHistory.getParsedColumnNames(), triggerHistory.getParsedPkColumnNames(), true);
            } else {
                throw new SymmetricException("Could not find the following table.  It might have been dropped: %s",
                        Table.getFullyQualifiedTableName(catalogName, schemaName, tableName));
            }
        } else {
            table = new Table(tableName);
            table.addColumns(triggerHistory.getParsedColumnNames());
            table.setPrimaryKeys(triggerHistory.getParsedPkColumnNames());
        }
        Router router = triggerRouterService.getRouterById(routerId, false);
        if (router != null && setTargetTableName) {
            if (router.isUseSourceCatalogSchema()) {
                table.setCatalog(catalogName);
                table.setSchema(schemaName);
            } else {
                table.setCatalog(null);
                table.setSchema(null);
            }
            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetCatalogName())) {
                table.setCatalog(null);
            } else if (StringUtils.isNotBlank(router.getTargetCatalogName())) {
                table.setCatalog(SymmetricUtils.replaceNodeVariables(sourceNode, targetNode, router.getTargetCatalogName()));
                table.setCatalog(SymmetricUtils.replaceCatalogSchemaVariables(catalogName, schemaName, router.getTargetCatalogName()));
            }
            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetSchemaName())) {
                table.setSchema(null);
            } else if (StringUtils.isNotBlank(router.getTargetSchemaName())) {
                table.setSchema(SymmetricUtils.replaceNodeVariables(sourceNode, targetNode, router.getTargetSchemaName()));
                table.setSchema(SymmetricUtils.replaceCatalogSchemaVariables(catalogName, schemaName, router.getTargetSchemaName()));
            }
            if (StringUtils.isNotBlank(router.getTargetTableName())) {
                table.setName(router.getTargetTableName());
            }
        }
        if (useTransforms) {
            TransformTable transform = getTransform(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), table,
                    TransformPoint.EXTRACT, Integer.MIN_VALUE);
            while (transform != null) {
                applyTransform(table, transform);
                transform = getTransform(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), table,
                        TransformPoint.EXTRACT, transform.getTransformOrder() + 1);
            }
            transform = getTransform(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), table,
                    TransformPoint.LOAD, Integer.MIN_VALUE);
            while (transform != null) {
                applyTransform(table, transform);
                transform = getTransform(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), table,
                        TransformPoint.LOAD, transform.getTransformOrder() + 1);
            }
        }
        return table;
    }

    protected IDatabasePlatform getTargetPlatform(String tableName) {
        return tableName.startsWith(tablePrefix) ? symmetricDialect.getPlatform() : symmetricDialect.getTargetDialect().getPlatform();
    }

    protected TransformTable getTransform(String sourceNodeGroupId, String targetNodeGroupId, Table table,
            TransformPoint transformPoint, int order) {
        List<TransformTableNodeGroupLink> transforms = transformService.findTransformsFor(sourceNode.getNodeGroupId(),
                targetNode.getNodeGroupId(), table.getName());
        if (transforms != null) {
            for (TransformTableNodeGroupLink transform : transforms) {
                if (StringUtils.equals(transform.getSourceCatalogName(), table.getCatalog())
                        && StringUtils.equals(transform.getSourceSchemaName(), table.getSchema())
                        && transform.getTransformPoint().equals(transformPoint) && transform.getTransformOrder() >= order) {
                    return transform;
                }
            }
        }
        return null;
    }

    protected void applyTransform(Table table, TransformTable transform) {
        List<String> columnNamesToRemoveList = new ArrayList<String>();
        if (transform.getColumnPolicy().equals(ColumnPolicy.SPECIFIED)) {
            columnNamesToRemoveList.addAll(Arrays.asList(table.getColumnNames()));
        }
        for (TransformColumn transformColumn : transform.getTransformColumns()) {
            if (StringUtils.isNotBlank(transformColumn.getSourceColumnName())) {
                Column column = table.getColumnWithName(transformColumn.getSourceColumnName());
                if (column != null) {
                    columnNamesToRemoveList.remove(column.getName());
                    column.setName(transformColumn.getTargetColumnName());
                    if (RemoveColumnTransform.NAME.equals(transformColumn.getTransformType())) {
                        columnNamesToRemoveList.add(column.getName());
                    } else {
                        columnNamesToRemoveList.remove(column.getName());
                    }
                    column.setPrimaryKey(transformColumn.isPk());
                }
            } else {
                Column column = new Column(transformColumn.getTargetColumnName());
                column.setPrimaryKey(transformColumn.isPk());
                column.setTypeCode(Types.VARCHAR);
                column.setJdbcTypeCode(Types.VARCHAR);
                column.setJdbcTypeName("VARCHAR");
                column.setSize("100");
                table.addColumn(column);
                columnNamesToRemoveList.remove(column.getName());
            }
        }
        for (String columnName : columnNamesToRemoveList) {
            table.removeColumn(table.getColumnIndex(columnName));
        }
        table.setCatalog(transform.getTargetCatalogName());
        table.setSchema(transform.getTargetSchemaName());
        table.setName(transform.getTargetTableName());
    }

    static class CacheKey {
        private String routerId;
        private int triggerHistoryId;
        private boolean setTargetTableName;
        private boolean useDatabaseDefinition;
        private boolean useTransforms;

        public CacheKey(String routerId, int triggerHistoryId, boolean setTargetTableName,
                boolean useDatabaseDefinition, boolean useTransforms) {
            this.routerId = routerId;
            this.triggerHistoryId = triggerHistoryId;
            this.setTargetTableName = setTargetTableName;
            this.useDatabaseDefinition = useDatabaseDefinition;
            this.useTransforms = useTransforms;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((routerId == null) ? 0 : routerId.hashCode());
            result = prime * result + (setTargetTableName ? 1231 : 1237);
            result = prime * result + triggerHistoryId;
            result = prime * result + (useDatabaseDefinition ? 1231 : 1237);
            result = prime * result + (useTransforms ? 1231 : 1237);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            if (routerId == null) {
                if (other.routerId != null) {
                    return false;
                }
            } else if (!routerId.equals(other.routerId)) {
                return false;
            }
            if (setTargetTableName != other.setTargetTableName) {
                return false;
            }
            if (triggerHistoryId != other.triggerHistoryId) {
                return false;
            }
            if (useDatabaseDefinition != other.useDatabaseDefinition) {
                return false;
            }
            if (useTransforms != other.useTransforms) {
                return false;
            }
            return true;
        }
    }
}
