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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.SymmetricUtils;

public class ColumnsAccordingToTriggerHistory {
    private static Map<String, Map<String, Table>> cacheByEngine = new ConcurrentHashMap<String, Map<String, Table>>();
    private Map<CacheKey, Table> cache = new HashMap<CacheKey, Table>();
    private ISymmetricEngine engine;
    private Node sourceNode;
    private Node targetNode;
    private ITriggerRouterService triggerRouterService;
    private ISymmetricDialect symmetricDialect;
    private String tablePrefix;
    private boolean isUsingTargetExternalId;

    public ColumnsAccordingToTriggerHistory(ISymmetricEngine engine, Node sourceNode, Node targetNode) {
        this.engine = engine;
        triggerRouterService = engine.getTriggerRouterService();
        symmetricDialect = engine.getSymmetricDialect();
        tablePrefix = engine.getTablePrefix().toLowerCase();
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        isUsingTargetExternalId = engine.getCacheManager().isUsingTargetExternalId(false);
    }

    public Table lookup(String routerId, TriggerHistory triggerHistory, boolean setTargetTableName,
            boolean useDatabaseDefinition, boolean addMissingColumns) {
        CacheKey key = new CacheKey(routerId, triggerHistory.getTriggerHistoryId(), setTargetTableName,
                useDatabaseDefinition, addMissingColumns);
        Table table = cache.get(key);
        if (table == null) {
            table = lookupAndOrderColumnsAccordingToTriggerHistory(routerId, triggerHistory, setTargetTableName,
                    useDatabaseDefinition, addMissingColumns);
            cache.put(key, table);
        }
        return table;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId, TriggerHistory triggerHistory,
            boolean setTargetTableName, boolean useDatabaseDefinition, boolean addMissingColumns) {
        String catalogName = triggerHistory.getSourceCatalogName();
        String schemaName = triggerHistory.getSourceSchemaName();
        String tableName = triggerHistory.getSourceTableName();
        String tableNameLowerCase = triggerHistory.getSourceTableNameLowerCase();
        Table table = null;
        if (useDatabaseDefinition) {
            if (isUsingTargetExternalId && !tableName.startsWith(tablePrefix)) {
                table = lookupTableExpanded(getTargetPlatform(tableNameLowerCase), catalogName, schemaName, tableName, triggerHistory,
                        addMissingColumns);
            } else {
                table = lookupTable(getTargetPlatform(tableNameLowerCase), catalogName, schemaName, tableName, triggerHistory, addMissingColumns);
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
                table.setCatalog(SymmetricUtils.replaceCatalogSchemaVariables(catalogName,
                        symmetricDialect.getTargetPlatform().getDefaultCatalog(),
                        schemaName,
                        symmetricDialect.getTargetPlatform().getDefaultSchema(),
                        router.getTargetCatalogName()));
            }
            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetSchemaName())) {
                table.setSchema(null);
            } else if (StringUtils.isNotBlank(router.getTargetSchemaName())) {
                table.setSchema(SymmetricUtils.replaceNodeVariables(sourceNode, targetNode, router.getTargetSchemaName()));
                table.setSchema(SymmetricUtils.replaceCatalogSchemaVariables(catalogName,
                        symmetricDialect.getTargetPlatform().getDefaultCatalog(),
                        schemaName,
                        symmetricDialect.getTargetPlatform().getDefaultSchema(),
                        router.getTargetSchemaName()));
            }
            if (StringUtils.isNotBlank(router.getTargetTableName())) {
                table.setName(router.getTargetTableName());
            }
        }
        return table;
    }

    protected IDatabasePlatform getTargetPlatform(String tableName) {
        return tableName.startsWith(tablePrefix) ? symmetricDialect.getPlatform() : symmetricDialect.getTargetDialect().getPlatform();
    }

    protected Table lookupTable(IDatabasePlatform platform, String catalogName, String schemaName, String tableName, TriggerHistory triggerHistory,
            boolean addMissingColumns) {
        Table table = platform.getTableFromCache(catalogName, schemaName, tableName, false);
        if (table != null && table.getColumnCount() < triggerHistory.getParsedColumnNames().length) {
            /*
             * If the column count is less than what trigger history reports, then chances are the table cache is out of date.
             */
            table = platform.getTableFromCache(catalogName, schemaName, tableName, true);
        }
        if (table != null) {
            table = table.copyAndFilterColumns(triggerHistory.getParsedColumnNames(),
                    triggerHistory.getParsedPkColumnNames(), true, addMissingColumns);
        } else {
            throw new SymmetricException("Could not find the following table.  It might have been dropped: %s",
                    Table.getFullyQualifiedTableName(catalogName, schemaName, tableName));
        }
        return table;
    }

    protected Table lookupTableExpanded(IDatabasePlatform platform, String catalogName, String schemaName, String tableName,
            TriggerHistory triggerHistory, boolean addMissingColumns) {
        Table table = null;
        if (!tableName.contains(targetNode.getExternalId())) {
            table = lookupTable(platform, catalogName, schemaName, tableName, triggerHistory, addMissingColumns);
        } else {
            String baseTableName = tableName.replace(targetNode.getExternalId(), "") + (addMissingColumns ? "-t" : "-f");
            Map<String, Table> sourceTableMap = getSourceTableMap(engine.getEngineName());
            table = sourceTableMap.get(baseTableName);
            if (table == null) {
                table = lookupTable(platform, catalogName, schemaName, tableName, triggerHistory, addMissingColumns);
                sourceTableMap.put(baseTableName, table);
            }
            if (table != null) {
                try {
                    table = (Table) table.clone();
                    table.setName(tableName);
                } catch (CloneNotSupportedException e) {
                }
            }
        }
        return table;
    }

    protected Map<String, Table> getSourceTableMap(String engineName) {
        Map<String, Table> map = cacheByEngine.get(engineName);
        if (map == null) {
            map = new ConcurrentHashMap<String, Table>();
            cacheByEngine.put(engineName, map);
        }
        return map;
    }

    static class CacheKey {
        private String routerId;
        private int triggerHistoryId;
        private boolean setTargetTableName;
        private boolean useDatabaseDefinition;
        private boolean addMissingColumns;

        public CacheKey(String routerId, int triggerHistoryId, boolean setTargetTableName,
                boolean useDatabaseDefinition, boolean addMissingColumns) {
            this.routerId = routerId;
            this.triggerHistoryId = triggerHistoryId;
            this.setTargetTableName = setTargetTableName;
            this.useDatabaseDefinition = useDatabaseDefinition;
            this.addMissingColumns = addMissingColumns;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((routerId == null) ? 0 : routerId.hashCode());
            result = prime * result + (setTargetTableName ? 1231 : 1237);
            result = prime * result + triggerHistoryId;
            result = prime * result + (useDatabaseDefinition ? 1231 : 1237);
            result = prime * result + (addMissingColumns ? 1231 : 1237);
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
            if (addMissingColumns != other.addMissingColumns) {
                return false;
            }
            return true;
        }
    }
}
