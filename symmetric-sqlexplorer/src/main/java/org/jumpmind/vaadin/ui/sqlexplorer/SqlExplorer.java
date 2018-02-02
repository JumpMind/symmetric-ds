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

import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_AUTO_COMPLETE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ConfirmDialog;
import org.jumpmind.vaadin.ui.common.ConfirmDialog.IConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.annotations.StyleSheet;
import com.vaadin.v7.data.Property.ValueChangeEvent;
import com.vaadin.v7.data.Property.ValueChangeListener;
import com.vaadin.server.FontAwesome;
import com.vaadin.server.Resource;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TabSheet.SelectedTabChangeEvent;
import com.vaadin.ui.TabSheet.SelectedTabChangeListener;
import com.vaadin.ui.TabSheet.Tab;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;

@StyleSheet({ "sqlexplorer.css" })
public class SqlExplorer extends HorizontalSplitPanel {

    private static final long serialVersionUID = 1L;

    final Logger log = LoggerFactory.getLogger(getClass());

    final static FontAwesome QUERY_ICON = FontAwesome.FILE_O;

    final static float DEFAULT_SPLIT_POS = 225;

    IDbProvider databaseProvider;

    ISettingsProvider settingsProvider;

    MenuItem showButton;

    DbTree dbTree;

    SqlExplorerTabPanel contentTabs;

    MenuBar contentMenuBar;

    IContentTab selected;

    float savedSplitPosition = DEFAULT_SPLIT_POS;

    String user;

    IDbMenuItem[] additionalMenuItems;

    Set<IInfoPanel> infoTabs = new HashSet<IInfoPanel>();

    public SqlExplorer(String configDir, IDbProvider databaseProvider, ISettingsProvider settingsProvider, String user) {
        this(configDir, databaseProvider, settingsProvider, user, DEFAULT_SPLIT_POS);
    }

    public SqlExplorer(String configDir, IDbProvider databaseProvider, String user, IDbMenuItem... additionalMenuItems) {
        this(configDir, databaseProvider, new DefaultSettingsProvider(configDir, user), user, DEFAULT_SPLIT_POS, additionalMenuItems);
    }

    public SqlExplorer(String configDir, IDbProvider databaseProvider, String user, float leftSplitPos) {
        this(configDir, databaseProvider, new DefaultSettingsProvider(configDir, user), user, leftSplitPos);
    }

    public SqlExplorer(String configDir, IDbProvider databaseProvider, ISettingsProvider settingsProvider, String user, float leftSplitSize,
            IDbMenuItem... additionalMenuItems) {
        this.databaseProvider = databaseProvider;
        this.settingsProvider = settingsProvider;
        this.savedSplitPosition = leftSplitSize;
        this.additionalMenuItems = additionalMenuItems;

        setSizeFull();
        addStyleName("sqlexplorer");

        VerticalLayout leftLayout = new VerticalLayout();
        leftLayout.setMargin(false);
        leftLayout.setSpacing(false);
        leftLayout.setSizeFull();
        leftLayout.addStyleName(ValoTheme.MENU_ROOT);

        leftLayout.addComponent(buildLeftMenu());

        Panel scrollable = new Panel();
        scrollable.setSizeFull();

        dbTree = buildDbTree();
        scrollable.setContent(dbTree);

        leftLayout.addComponent(scrollable);
        leftLayout.setExpandRatio(scrollable, 1);

        VerticalLayout rightLayout = new VerticalLayout();
        rightLayout.setMargin(false);
        rightLayout.setSpacing(false);
        rightLayout.setSizeFull();

        VerticalLayout rightMenuWrapper = new VerticalLayout();
        rightMenuWrapper.setMargin(false);
        rightMenuWrapper.setWidth(100, Unit.PERCENTAGE);
        rightMenuWrapper.addStyleName(ValoTheme.MENU_ROOT);
        contentMenuBar = new MenuBar();
        contentMenuBar.addStyleName(ValoTheme.MENUBAR_BORDERLESS);
        contentMenuBar.setWidth(100, Unit.PERCENTAGE);
        addShowButton(contentMenuBar);

        rightMenuWrapper.addComponent(contentMenuBar);
        rightLayout.addComponent(rightMenuWrapper);

        contentTabs = new SqlExplorerTabPanel();
        contentTabs.addSelectedTabChangeListener(new SelectedTabChangeListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void selectedTabChange(SelectedTabChangeEvent event) {
                selectContentTab((IContentTab) contentTabs.getSelectedTab());
            }
        });
        rightLayout.addComponent(contentTabs);
        rightLayout.setExpandRatio(contentTabs, 1);

        addComponents(leftLayout, rightLayout);

        setSplitPosition(savedSplitPosition, Unit.PIXELS);
    }

    protected MenuBar buildLeftMenu() {
        MenuBar leftMenu = new MenuBar();
        leftMenu.addStyleName(ValoTheme.MENUBAR_BORDERLESS);
        leftMenu.setWidth(100, Unit.PERCENTAGE);
        MenuItem hideButton = leftMenu.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                savedSplitPosition = getSplitPosition() > 10 ? getSplitPosition() : DEFAULT_SPLIT_POS;
                setSplitPosition(0);
                setLocked(true);
                showButton.setVisible(true);
            }
        });
        hideButton.setDescription("Hide the database explorer");
        hideButton.setIcon(FontAwesome.BARS);

        MenuItem refreshButton = leftMenu.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                dbTree.refresh();
                Component tab = contentTabs.getSelectedTab();
                if (tab instanceof QueryPanel) {
                    if (findQueryPanelForDb(((QueryPanel) tab).db).suggester != null) {
                        findQueryPanelForDb(((QueryPanel) tab).db).suggester.clearCaches();
                    }
                }
            }
        });
        refreshButton.setIcon(FontAwesome.REFRESH);
        refreshButton.setDescription("Refresh the database explorer");

        MenuItem openQueryTab = leftMenu.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                openQueryWindow(dbTree.getSelected());
            }
        });
        openQueryTab.setIcon(QUERY_ICON);
        openQueryTab.setDescription("Open a query tab");

        MenuItem settings = leftMenu.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                SettingsDialog dialog = new SettingsDialog(SqlExplorer.this);
                dialog.showAtSize(.5);
            }
        });
        settings.setIcon(FontAwesome.GEAR);
        settings.setDescription("Modify sql explorer settings");
        return leftMenu;
    }

    protected void addShowButton(MenuBar contentMenuBar) {
        boolean visible = showButton != null ? showButton.isVisible() : false;
        showButton = contentMenuBar.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                setSplitPosition(savedSplitPosition, Unit.PIXELS);
                setLocked(false);
                showButton.setVisible(false);
            }
        });
        showButton.setIcon(FontAwesome.BARS);
        showButton.setDescription("Show the database explorer");
        showButton.setVisible(visible);
    }

    protected void selectContentTab(IContentTab tab) {
        if (selected != null) {
            selected.unselected();
        }
        contentTabs.setSelectedTab(tab);
        contentMenuBar.removeItems();
        addShowButton(contentMenuBar);
        if (tab instanceof QueryPanel) {
            ((DefaultButtonBar) ((QueryPanel) tab).getButtonBar()).populate(contentMenuBar);
        }
        tab.selected();
        selected = tab;
    }

    protected QueryPanel openQueryWindow(DbTreeNode node) {
        return openQueryWindow(dbTree.getDbForNode(node));
    }

    protected QueryPanel openQueryWindow(IDb db) {
        String dbName = db.getName();
        DefaultButtonBar buttonBar = new DefaultButtonBar();
        QueryPanel panel = new QueryPanel(db, settingsProvider, buttonBar, user);
        buttonBar.init(db, settingsProvider, panel, additionalMenuItems);
        Tab tab = contentTabs.addTab(panel, getTabName(dbName));
        tab.setClosable(true);
        tab.setIcon(QUERY_ICON);
        selectContentTab(panel);
        return panel;
    }

    protected void openQueryWindow(Set<DbTreeNode> nodes) {
        Set<String> dbNames = new HashSet<String>();
        for (DbTreeNode node : nodes) {
            IDb db = dbTree.getDbForNode(node);
            String dbName = db.getName();
            if (!dbNames.contains(dbName)) {
                dbNames.add(dbName);
                openQueryWindow(node);
            }
        }
    }

    public void refreshQueryPanels() {
        for (Component panel : contentTabs) {
            if (panel instanceof QueryPanel) {
                QueryPanel queryPanel = ((QueryPanel) panel);

                if (settingsProvider.get().getProperties().is(SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS)) {
                    queryPanel.removeGeneralResultsTab();
                } else if (!settingsProvider.get().getProperties().is(SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS)) {
                    queryPanel.createGeneralResultsTab();
                }
                boolean autoCompleteEnabled = settingsProvider.get().getProperties().is(SQL_EXPLORER_AUTO_COMPLETE);
                queryPanel.setAutoCompleteEnabled(autoCompleteEnabled);
            }
        }
    }

    public QueryPanel findQueryPanelForDb(IDb db) {
        QueryPanel panel = null;
        if (contentTabs.getComponentCount() > 0) {
            Component comp = contentTabs.getSelectedTab();
            if (comp instanceof QueryPanel) {
                QueryPanel prospectiveQueryPanel = (QueryPanel) comp;
                if (prospectiveQueryPanel.getDb().getName().equals(db.getName())) {
                    panel = prospectiveQueryPanel;
                }
            }

            if (panel == null) {
                Iterator<Component> i = contentTabs.iterator();
                while (i.hasNext()) {
                    comp = (Component) i.next();
                    if (comp instanceof QueryPanel) {
                        QueryPanel prospectiveQueryPanel = (QueryPanel) comp;
                        if (prospectiveQueryPanel.getDb().getName().equals(db.getName())) {
                            panel = prospectiveQueryPanel;
                            break;
                        }
                    }
                }
            }

            if (panel == null) {
                panel = openQueryWindow(db);
            }
        }

        return panel;
    }

    protected void generateSelectForSelectedTables() {
        Set<DbTreeNode> tableNodes = dbTree.getSelected(DbTree.NODE_TYPE_TABLE);
        for (DbTreeNode treeNode : tableNodes) {
            IDb db = dbTree.getDbForNode(treeNode);
            QueryPanel panel = findQueryPanelForDb(db);
            IDatabasePlatform platform = db.getPlatform();
            Table table = treeNode.getTableFor();
            DmlStatement dmlStatement = platform.createDmlStatement(DmlType.SELECT_ALL, table, null);
            panel.appendSql(dmlStatement.getSql());
            contentTabs.setSelectedTab(panel);
        }
    }

    protected void generateDmlForSelectedTables(DmlType dmlType) {
        Set<DbTreeNode> tableNodes = dbTree.getSelected(DbTree.NODE_TYPE_TABLE);
        for (DbTreeNode treeNode : tableNodes) {
            IDb db = dbTree.getDbForNode(treeNode);
            QueryPanel panel = findQueryPanelForDb(db);
            IDatabasePlatform platform = db.getPlatform();
            Table table = treeNode.getTableFor();
            DmlStatement dmlStatement = platform.createDmlStatement(dmlType, table, null);
            Row row = new Row(table.getColumnCount());
            Column[] columns = table.getColumns();
            for (Column column : columns) {
                String value = null;
                if (column.getParsedDefaultValue() == null) {
                    value = CommonUiUtils.getJdbcTypeValue(column.getJdbcTypeName());
                } else {
                    value = column.getParsedDefaultValue().toString();
                }
                row.put(column.getName(), value);

            }
            String sql = dmlStatement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
            panel.appendSql(sql);
            contentTabs.setSelectedTab(panel);
        }
    }

    protected void dropSelectedTables() {
        Set<DbTreeNode> tableNodes = dbTree.getSelected(DbTree.NODE_TYPE_TABLE);
        List<Table> tables = new ArrayList<Table>();
        Map<Table, DbTreeNode> tableToTreeNode = new HashMap<Table, DbTreeNode>();
        for (DbTreeNode treeNode : tableNodes) {
            Table table = treeNode.getTableFor();
            tables.add(table);
            tableToTreeNode.put(table, treeNode);
        }

        tables = Database.sortByForeignKeys(tables);
        Collections.reverse(tables);
        dropTables(tables, tableToTreeNode);
    }

    private void dropTables(final List<Table> tables, final Map<Table, DbTreeNode> tableToTreeNode) {
        String msg = null;
        if (tables.size() > 1) {
            msg = "Do you want to drop " + tables.size() + " tables?";
        } else if (tables.size() == 1) {
            Table table = tables.get(0);
            msg = "Do you want to drop " + table.getFullyQualifiedTableName() + "?";
        }
        ConfirmDialog.show("Drop Tables?", msg, new IConfirmListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean onOk() {
                for (Table table : tables) {
                    DbTreeNode treeNode = tableToTreeNode.get(table);
                    IDb db = dbTree.getDbForNode(treeNode);
                    try {
                        db.getPlatform().dropTables(false, table);
                    } catch (Exception e) {
                        String msg = "Failed to drop " + table.getFullyQualifiedTableName() + ".  ";
                        CommonUiUtils.notify(msg + "See log file for more details", Type.WARNING_MESSAGE);
                        log.warn(msg, e);
                    }
                }
                for (IContentTab panel : infoTabs) {
                    contentTabs.removeComponent(panel);
                }
                infoTabs.clear();
                dbTree.refresh();
                return true;

            }
        });
    }

    protected DbTree buildDbTree() {

        final DbTree tree = new DbTree(databaseProvider, settingsProvider);
        tree.addValueChangeListener(new ValueChangeListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void valueChange(ValueChangeEvent event) {
                Set<DbTreeNode> nodes = dbTree.getSelected();
                if (nodes != null) {
                    for (DbTreeNode treeNode : nodes) {
                        IDb db = dbTree.getDbForNode(treeNode);
                        QueryPanel panel = getQueryPanelForDb(db);
                        if (panel == null && db != null) {
                            openQueryWindow(db);
                        }
                    }

                    String selectedTabCaption = null;
                    for (IInfoPanel panel : infoTabs) {
                        selectedTabCaption = panel.getSelectedTabCaption();
                        contentTabs.removeComponent(panel);
                    }
                    infoTabs.clear();

                    if (nodes.size() > 0) {
                        DbTreeNode treeNode = nodes.iterator().next();
                        if (treeNode != null && treeNode.getType().equals(DbTree.NODE_TYPE_DATABASE)) {
                            IDb db = dbTree.getDbForNode(treeNode);
                            DatabaseInfoPanel databaseInfoTab = new DatabaseInfoPanel(db, settingsProvider.get(), selectedTabCaption);
                            Tab tab = contentTabs.addTab(databaseInfoTab, db.getName(), FontAwesome.DATABASE, 0);
                            tab.setClosable(true);
                            selectContentTab(databaseInfoTab);
                            infoTabs.add(databaseInfoTab);
                        }
                        if (treeNode != null && treeNode.getType().equals(DbTree.NODE_TYPE_TABLE)) {
                            Table table = treeNode.getTableFor();
                            if (table != null) {
                                IDb db = dbTree.getDbForNode(treeNode);
                                TableInfoPanel tableInfoTab = new TableInfoPanel(table, user, db, settingsProvider.get(), SqlExplorer.this,
                                        selectedTabCaption);
                                Tab tab = contentTabs.addTab(tableInfoTab, table.getFullyQualifiedTableName(), FontAwesome.TABLE, 0);
                                tab.setClosable(true);
                                selectContentTab(tableInfoTab);
                                infoTabs.add(tableInfoTab);
                            }
                        } else if (treeNode != null && treeNode.getType().equals(DbTree.NODE_TYPE_TRIGGER)) {
                            Table table = treeNode.getParent().getTableFor();
                            IDdlReader reader = dbTree.getDbForNode(treeNode).getPlatform().getDdlReader();
                            Trigger trigger = reader.getTriggerFor(table, treeNode.getName());
                            if (trigger != null) {
                                IDb db = dbTree.getDbForNode(treeNode);
                                TriggerInfoPanel triggerInfoTab = new TriggerInfoPanel(trigger, db, settingsProvider.get(),
                                        selectedTabCaption);
                                Tab tab = contentTabs.addTab(triggerInfoTab, trigger.getName(), FontAwesome.CROSSHAIRS, 0);
                                tab.setClosable(true);
                                selectContentTab(triggerInfoTab);
                                infoTabs.add(triggerInfoTab);
                            }
                        }
                    }
                }
            }
        });
        tree.registerAction(new DbTreeAction("Query", QUERY_ICON) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                openQueryWindow(nodes);
            }
        }, DbTree.NODE_TYPE_DATABASE, DbTree.NODE_TYPE_CATALOG, DbTree.NODE_TYPE_SCHEMA, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Select", QUERY_ICON) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                generateSelectForSelectedTables();
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Insert", QUERY_ICON) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                generateDmlForSelectedTables(DmlType.INSERT);
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Update", QUERY_ICON) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                generateDmlForSelectedTables(DmlType.UPDATE);
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Delete", QUERY_ICON) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                generateDmlForSelectedTables(DmlType.DELETE);
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Drop", FontAwesome.ARROW_DOWN) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                dropSelectedTables();
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Import", FontAwesome.DOWNLOAD) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                if (nodes.size() > 0) {
                    IDb db = dbTree.getDbForNode(nodes.iterator().next());
                    new DbImportDialog(db.getPlatform(), dbTree.getSelectedTables()).showAtSize(0.6);
                }
            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Export", FontAwesome.UPLOAD) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                if (nodes.size() > 0) {
                    IDb db = dbTree.getDbForNode(nodes.iterator().next());
                    new DbExportDialog(db.getPlatform(), dbTree.getSelectedTables(), findQueryPanelForDb(db)).showAtSize(0.6);
                }
            }
        }, DbTree.NODE_TYPE_TABLE, DbTree.NODE_TYPE_TRIGGER);

        tree.registerAction(new DbTreeAction("Fill", FontAwesome.BEER) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {
                if (nodes.size() > 0) {
                    IDb db = dbTree.getDbForNode(nodes.iterator().next());
                    new DbFillDialog(db.getPlatform(), dbTree.getSelectedTables(), findQueryPanelForDb(db)).showAtSize(0.6);
                }

            }
        }, DbTree.NODE_TYPE_TABLE);

        tree.registerAction(new DbTreeAction("Copy Name", FontAwesome.COPY) {
            private static final long serialVersionUID = 1L;

            @Override
            public void handle(Set<DbTreeNode> nodes) {

                for (DbTreeNode treeNode : nodes) {
                    IDb db = dbTree.getDbForNode(nodes.iterator().next());
                    DatabaseInfo dbInfo = db.getPlatform().getDatabaseInfo();
                    final String quote = dbInfo.getDelimiterToken();
                    final String catalogSeparator = dbInfo.getCatalogSeparator();
                    final String schemaSeparator = dbInfo.getSchemaSeparator();

                    Table table = treeNode.getTableFor();
                    if (table != null) {
                        QueryPanel panel = findQueryPanelForDb(db);
                        panel.appendSql(table.getQualifiedTableName(quote, catalogSeparator, schemaSeparator));
                        contentTabs.setSelectedTab(panel);
                    }
                }
            }
        }, DbTree.NODE_TYPE_TABLE);

        return tree;

    }

    protected QueryPanel getQueryPanelForDb(IDb db) {
        if (db != null) {
            Iterator<Component> i = contentTabs.iterator();
            while (i.hasNext()) {
                Component c = i.next();
                if (c instanceof QueryPanel) {
                    QueryPanel panel = (QueryPanel) c;
                    if (panel.getDb().getName().equals(db.getName())) {
                        return panel;
                    }
                }
            }
        }
        return null;
    }

    protected String getTabName(String name) {
        int tabs = contentTabs.getComponentCount();
        String tabName = tabs > 0 ? null : name;
        if (tabName == null) {
            for (int j = 0; j < 10; j++) {
                boolean alreadyUsed = false;
                String suffix = "";
                for (int i = 0; i < tabs; i++) {
                    Tab tab = contentTabs.getTab(i);
                    String currentTabName = tab.getCaption();

                    if (j > 0) {
                        suffix = "-" + j;
                    }
                    if (currentTabName.equals(name + suffix)) {
                        alreadyUsed = true;
                    }
                }

                if (!alreadyUsed) {
                    tabName = name + suffix;
                    break;
                }
            }
        }
        return tabName;
    }

    public ISettingsProvider getSettingsProvider() {
        return settingsProvider;
    }

    public IDbProvider getDatabaseProvider() {
        return databaseProvider;
    }

    public void refresh() {
        dbTree.refresh();
    }

    public void focus() {
        dbTree.focus();
    }

    public void addResultsTab(String caption, Resource icon, IContentTab panel) {
        Tab tab = contentTabs.addTab(panel, caption);
        tab.setClosable(true);
        tab.setIcon(icon);
        selectContentTab(panel);
    }

    public void putResultsInQueryTab(String value, IDb db) {
        openQueryWindow(db).appendSql(value);
    }

}
