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
package org.jumpmind.vaadin.ui.sqlexplorer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.data.TreeData;
import com.vaadin.data.provider.TreeDataProvider;
import com.vaadin.icons.VaadinIcons;
import com.vaadin.ui.Tree;
import com.vaadin.ui.StyleGenerator;

public class DbTree extends Tree<DbTreeNode> {

    public static final String PROPERTY_SCHEMA_NAME = "schemaName";
    public static final String PROPERTY_CATALOG_NAME = "catalogName";

    private static final long serialVersionUID = 1L;

    final Logger log = LoggerFactory.getLogger(getClass());

    public final static String[] TABLE_TYPES = new String[] { "TABLE",
            "SYSTEM TABLE", "SYSTEM VIEW" };

    public static final String NODE_TYPE_DATABASE = "Database";
    public static final String NODE_TYPE_CATALOG = "Catalog";
    public static final String NODE_TYPE_SCHEMA = "Schema";
    public static final String NODE_TYPE_TABLE = "Table";
    public static final String NODE_TYPE_TRIGGER = "Trigger";

    IDbProvider databaseProvider;

    ISettingsProvider settingsProvider;
    
    TreeData<DbTreeNode> treeData;
    
    TreeDataProvider<DbTreeNode> treeDataProvider;

    public DbTree(IDbProvider databaseProvider, ISettingsProvider settingsProvider) {
        this.databaseProvider = databaseProvider;
        this.settingsProvider = settingsProvider;
        setWidth(100, Unit.PERCENTAGE);
        setStyleGenerator(getStyleGenerator());
        setStyleGenerator(new DbTreeStyleGenerator());
        setItemIconGenerator(node -> node.getIcon());
    }

    public void refresh() {
        List<IDb> databases = databaseProvider.getDatabases();
        Set<DbTreeNode> selected = getSelectedItems();
        if (treeData == null) {
            treeData = new TreeData<DbTreeNode>();
        } else {
            treeData.clear();
        }
        DbTreeNode firstNode = null;
        for (IDb database : databases) {
            DbTreeNode databaseNode = new DbTreeNode(this, database.getName(), NODE_TYPE_DATABASE, VaadinIcons.DATABASE, null);
            treeData.addItem(null, databaseNode);
            loadChildren(databaseNode);

            if (firstNode == null) {
                firstNode = databaseNode;
            }
        }
        
        treeDataProvider = new TreeDataProvider<DbTreeNode>(treeData);
        setDataProvider(treeDataProvider);

        if (selected == null || selected.size() == 0) {
            selected = new HashSet<DbTreeNode>();
            selected.add(firstNode);
        }
        
        for (DbTreeNode node : selected) {
            select(node);
        }

        focus();

    }

    public Set<DbTreeNode> getSelected(String type) {
        HashSet<DbTreeNode> nodes = new HashSet<DbTreeNode>();
        Set<DbTreeNode> selected = getSelectedItems();
        for (DbTreeNode treeNode : selected) {
            if (treeNode.getType().equals(type)) {
                nodes.add(treeNode);
            }
        }
        return nodes;
    }

    public Set<Table> getSelectedTables() {
        Set<Table> tables = new HashSet<Table>();
        for (DbTreeNode treeNode : getSelectedItems()) {
            Table table = treeNode.getTableFor();
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    protected IDb getDbForNode(DbTreeNode node) {
        while (node.getParent() != null) {
            node = node.getParent();
        }
        String databaseName = node.getName();
        List<IDb> databases = databaseProvider.getDatabases();
        for (IDb database : databases) {
            if (database.getName().equals(databaseName)) {
                return database;
            }
        }
        return null;
    }
    
    protected void loadChildren(DbTreeNode treeNode) {
        try {
            IDatabasePlatform platform = getDbForNode(treeNode).getPlatform();
            IDdlReader reader = platform.getDdlReader();

            Collection<?> children = treeData.getChildren(treeNode);
            if (children == null || children.size() == 0) {
                if (treeNode.getType().equals(NODE_TYPE_DATABASE)) {
                    List<String> catalogs = reader.getCatalogNames();
                    if (catalogs.size() > 0) {
                        addCatalogNodes(reader,treeNode,platform,catalogs);
                    } else {
                        List<String> schemas = reader.getSchemaNames(null);
                        addSchemaNodes(reader,treeNode,platform,schemas);
                    }

                    if (treeNode.getChildren().size() == 0) {
                        addTableNodes(reader,treeNode,null,null);
                    }
                } else if (treeNode.getType().equals(NODE_TYPE_CATALOG)) {
                    List<String> schemas = reader.getSchemaNames(treeNode.getName());
                    addSchemaNodes(reader,treeNode,platform,schemas);
                    
                    if(treeNode.getChildren().size() == 0){
                        addTableNodes(reader, treeNode, treeNode.getName(), null);
                    }

                } else if (treeNode.getType().equals(NODE_TYPE_SCHEMA)) {
                    String catalogName = null;
                    DbTreeNode parent = treeData.getParent(treeNode);
                    if (parent != null && parent.getType().equals(NODE_TYPE_CATALOG)) {
                        catalogName = parent.getName();
                    }
                    addTableNodes(reader, treeNode, catalogName,
                            treeNode.getName());
                } else if (treeNode.getType().equals(NODE_TYPE_TABLE)) {
                    String catalogName = null, schemaName = null;
                    DbTreeNode parent = treeData.getParent(treeNode);
                    if (parent != null && parent.getType().equals(NODE_TYPE_SCHEMA)) {
                        schemaName = parent.getName();
                        DbTreeNode grandparent = treeData.getParent(parent);
                        if (grandparent != null && grandparent.getType().equals(NODE_TYPE_CATALOG)) {
                            catalogName = grandparent.getName();
                        }
                    } else if (parent != null && parent.getType().equals(NODE_TYPE_CATALOG)) {
                        catalogName = parent.getName();
                    }
                    addTriggerNodes(reader, treeNode, catalogName, schemaName);
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            CommonUiUtils.notify(ex);
        }
    }

    protected List<DbTreeNode> getTableTreeNodes(IDdlReader reader,
            DbTreeNode parent, String catalogName, String schemaName) {
        List<DbTreeNode> list = new ArrayList<DbTreeNode>();
        List<String> tables = reader.getTableNames(catalogName, schemaName, TABLE_TYPES);        
        Collections.sort(tables, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.toUpperCase().compareTo(o2.toUpperCase());
            }
        });
        
        for (String tableName : tables) {
            String excludeRegex = settingsProvider.get().getProperties().get(Settings.SQL_EXPLORER_EXCLUDE_TABLES_REGEX);
            if (!tableName.matches(excludeRegex)
                    && !tableName.toUpperCase().matches(excludeRegex)
                    && !tableName.toLowerCase().matches(excludeRegex)) {

                DbTreeNode treeNode = new DbTreeNode(this, tableName,
                        NODE_TYPE_TABLE, VaadinIcons.TABLE, parent);
                if (catalogName != null) {
                    treeNode.getProperties().setProperty(PROPERTY_CATALOG_NAME, catalogName);
                }
                if (schemaName != null) {
                    treeNode.getProperties().setProperty(PROPERTY_SCHEMA_NAME, schemaName);
                }

                list.add(treeNode);
            }
        }
        return list;
    }

    protected void addTableNodes(IDdlReader reader, DbTreeNode parent,
            String catalogName, String schemaName) {
        List<DbTreeNode> nodes = getTableTreeNodes(reader, parent, catalogName, schemaName);
        for (DbTreeNode treeNode : nodes) {
            parent.getChildren().add(treeNode);
            treeData.addItem(treeNode.getParent(), treeNode);
            loadChildren(treeNode);
        }
    }

    protected List<DbTreeNode> getTriggerTreeNodes(IDdlReader reader,
            DbTreeNode parent, String catalogName, String schemaName) {
        List<DbTreeNode> list = new ArrayList<DbTreeNode>();
        List<Trigger> triggers = new ArrayList<Trigger>();
        try {
            triggers = reader.getTriggers(catalogName, schemaName, parent.getName());
        } catch (Exception e) {
            log.warn("Unable to look up triggers for catalog, schema, table : " + catalogName + "." + schemaName + "." + parent.getName());
        }
        for (Trigger trigger : triggers) {
            DbTreeNode treeNode = new DbTreeNode(this, trigger.getName(), NODE_TYPE_TRIGGER, VaadinIcons.CROSSHAIRS, parent);
            if (catalogName != null) {
                treeNode.getProperties().setProperty(PROPERTY_CATALOG_NAME, catalogName);
            }
            if (schemaName != null) {
                treeNode.getProperties().setProperty(PROPERTY_SCHEMA_NAME, schemaName);
            }
            list.add(treeNode);
        }
        return list;
    }

    protected void addTriggerNodes(IDdlReader reader, DbTreeNode parent, String catalogName, String schemaName) {
        List<DbTreeNode> nodes = getTriggerTreeNodes(reader, parent, catalogName, schemaName);
        for (DbTreeNode treeNode : nodes) {
            parent.getChildren().add(treeNode);
            treeData.addItem(treeNode.getParent(), treeNode);
        }
    }
    
    protected void addCatalogNodes(IDdlReader reader, DbTreeNode parent, IDatabasePlatform platform, List<String> catalogs){
        Collections.sort(catalogs);
        if (catalogs.remove(platform.getDefaultCatalog())) {
            catalogs.add(0, platform.getDefaultCatalog());
        }
        for (String catalog : catalogs) {
            DbTreeNode catalogNode = new DbTreeNode(this, catalog, NODE_TYPE_CATALOG, VaadinIcons.BOOK, parent);
            parent.getChildren().add(catalogNode);
            treeData.addItem(catalogNode.getParent(), catalogNode);
            loadChildren(catalogNode);
        }
    }
    
    protected void addSchemaNodes(IDdlReader reader, DbTreeNode parent, IDatabasePlatform platform,List<String> schemas){
        Collections.sort(schemas);
        if (schemas.remove(platform.getDefaultSchema())) {
            schemas.add(0, platform.getDefaultSchema());
        }
        for (String schema : schemas) {
            DbTreeNode schemaNode = new DbTreeNode(this, schema, NODE_TYPE_SCHEMA, VaadinIcons.BOOK, parent);
            parent.getChildren().add(schemaNode);
            treeData.addItem(schemaNode.getParent(), schemaNode);
            loadChildren(schemaNode);
        }
    }

    class DbTreeStyleGenerator implements StyleGenerator<DbTreeNode> {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(DbTreeNode node) {
            try {
                if (node.getType().equals(NODE_TYPE_CATALOG)) {
                    IDatabasePlatform platform = getDbForNode(node).getPlatform();
                    String catalog = platform.getDefaultCatalog();
                    if (catalog != null && catalog.equals(node.getName())) {
                        return "bold";
                    }
                } else if (node.getType().equals(NODE_TYPE_SCHEMA)) {
                    IDatabasePlatform platform = getDbForNode(node).getPlatform();
                    String schema = platform.getDefaultSchema();
                    if (schema != null && schema.equals(node.getName())) {
                        return "bold";
                    }
                }
            } catch (Exception e) {
                log.error("Failed to see if this node is the default catalog and/or schema", e);
            }
            return null;
        }
    }

}
