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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_MAX_RESULTS;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_SHOW_ROW_NUMBERS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.CsvExport;
import org.jumpmind.vaadin.ui.common.GridDataProvider;
import org.jumpmind.vaadin.ui.common.IDataProvider;
import org.jumpmind.vaadin.ui.common.ReadOnlyTextAreaDialog;
import org.jumpmind.vaadin.ui.sqlexplorer.SqlRunner.ISqlRunnerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.contextmenu.ContextMenu;
import com.vaadin.data.provider.Query;
import com.vaadin.icons.VaadinIcons;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Label;

public class TabularResultLayout extends VerticalLayout {

    private static final long serialVersionUID = 1L;

    final String ACTION_SELECT = "Select From";

    final String ACTION_INSERT = "Insert";

    final String ACTION_UPDATE = "Update";

    final String ACTION_DELETE = "Delete";

    final Logger log = LoggerFactory.getLogger(getClass());

    SqlExplorer explorer;

    QueryPanel queryPanel;

    String tableName;

    String catalogName;

    String schemaName;

    Grid<List<Object>> grid;

    org.jumpmind.db.model.Table resultTable;

    String sql;

    ResultSet rs;

    IDb db;

    ISqlRunnerListener listener;

    String user;

    Settings settings;

    boolean showSql = true;

    boolean isInQueryGeneralResults;

    MenuBar.MenuItem followToMenu;

    MenuBar.MenuItem toggleKeepResultsButton;

    Label resultLabel;

    public TabularResultLayout(IDb db, String sql, ResultSet rs, ISqlRunnerListener listener, Settings settings, boolean showSql)
            throws SQLException {
        this(null, db, sql, rs, listener, null, settings, null, showSql, false);
    }

    public TabularResultLayout(SqlExplorer explorer, IDb db, String sql, ResultSet rs, ISqlRunnerListener listener, String user,
            Settings settings, QueryPanel queryPanel, boolean showSql, boolean isInQueryGeneralResults) throws SQLException {
        this.explorer = explorer;
        this.sql = sql;
        this.showSql = showSql;
        this.db = db;
        this.rs = rs;
        this.listener = listener;
        this.user = user;
        this.settings = settings;
        this.queryPanel = queryPanel;
        this.isInQueryGeneralResults = isInQueryGeneralResults;
        createTabularResultLayout();
    }

    public String getSql() {
        return sql;
    }

    public void setShowSql(boolean showSql) {
        this.showSql = showSql;
    }

    protected void createTabularResultLayout() {
        this.setSizeFull();
        this.setSpacing(false);
        this.setMargin(false);
        createMenuBar();

        try {
            grid = putResultsInGrid(settings.getProperties().getInt(SQL_EXPLORER_MAX_RESULTS));
            grid.setSizeFull();

            ContextMenu menu = new ContextMenu(grid, true);
            menu.addItem(ACTION_SELECT, new MenuBar.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(MenuItem selectedItem) {
                    handleAction(ACTION_SELECT);
                }
            });
            menu.addItem(ACTION_INSERT, new MenuBar.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(MenuItem selectedItem) {
                    handleAction(ACTION_INSERT);
                }
            });
            menu.addItem(ACTION_UPDATE, new MenuBar.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(MenuItem selectedItem) {
                    handleAction(ACTION_UPDATE);
                }
            });
            menu.addItem(ACTION_DELETE, new MenuBar.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(MenuItem selectedItem) {
                    handleAction(ACTION_DELETE);
                }
            });

            if (resultTable != null && resultTable.getForeignKeyCount() > 0) {
                followToMenu = menu.addItem("Follow to", null);
                buildFollowToMenu();
            }

            grid.addItemClickListener(event -> {
                MouseButton button = event.getMouseEventDetails().getButton();
                if (button == MouseButton.LEFT) {
                    if (event.getMouseEventDetails().isDoubleClick() && event.getColumn() != null) {
                        String header = event.getColumn().getCaption();
                        List<Grid.Column<List<Object>, ?>> colList = grid.getColumns();
                        Object o = null;
                        for (int i = 1; i < colList.size(); i++) {
                            if (colList.get(i).getCaption().equals(header)) {
                                o = event.getItem().get(i - 1);
                                break;
                            }
                        }
                        if (o != null) {
                            String data = String.valueOf(o);
                            boolean binary = resultTable != null ? resultTable.getColumnWithName(header).isOfBinaryType() : false;
                            if (binary) {
                                ReadOnlyTextAreaDialog.show(header, data.toUpperCase(), binary);
                            } else {
                                ReadOnlyTextAreaDialog.show(header, data, binary);
                            }
                        }
                    } else {
                        grid.deselectAll();
                        grid.select(event.getItem());
                    }
                }
            });
            
            this.addComponent(grid);
            this.setExpandRatio(grid, 1);

            long count = (grid.getDataProvider().fetch(new Query<>()).count());
            int maxResultsSize = settings.getProperties().getInt(SQL_EXPLORER_MAX_RESULTS);
            if (count >= maxResultsSize) {
                resultLabel.setValue("Limited to <span style='color: red'>" + maxResultsSize + "</span> rows;");
            } else {
                resultLabel.setValue(count + " rows returned;");
            }
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
            CommonUiUtils.notify(ex);
        }

    }

    private void createMenuBar() {
        HorizontalLayout resultBar = new HorizontalLayout();
        resultBar.setWidth(100, Unit.PERCENTAGE);
        resultBar.setMargin(new MarginInfo(false, true, false, true));

        HorizontalLayout leftBar = new HorizontalLayout();
        leftBar.setSpacing(true);
        resultLabel = new Label("", ContentMode.HTML);
        leftBar.addComponent(resultLabel);

        final Label sqlLabel = new Label("", ContentMode.TEXT);
        sqlLabel.setWidth(800, Unit.PIXELS);
        leftBar.addComponent(sqlLabel);

        resultBar.addComponent(leftBar);
        resultBar.setComponentAlignment(leftBar, Alignment.MIDDLE_LEFT);
        resultBar.setExpandRatio(leftBar, 1);

        MenuBar rightBar = new MenuBar();
        rightBar.addStyleName(ValoTheme.MENUBAR_BORDERLESS);
        rightBar.addStyleName(ValoTheme.MENUBAR_SMALL);

        MenuBar.MenuItem refreshButton = rightBar.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuBar.MenuItem selectedItem) {
                listener.reExecute(sql);
            }
        });
        refreshButton.setIcon(VaadinIcons.REFRESH);
        refreshButton.setDescription("Refresh");
        
        MenuBar.MenuItem exportButton = rightBar.addItem("", new Command() {
            private static final long serialVersionUID = 1L;
            
            @Override
            public void menuSelected(MenuBar.MenuItem selectedItem) {
                IDataProvider target = new GridDataProvider(grid);
                CsvExport csvExport = null;
                if (target instanceof IDataProvider) {
                    csvExport = new CsvExport((IDataProvider) target);
                    csvExport.setFileName(db.getName() + "-export.csv");
                    csvExport.setTitle(sql);
                    csvExport.export();
                }

            }
        });
        exportButton.setIcon(VaadinIcons.UPLOAD);
        exportButton.setDescription("Export Results");

        if (isInQueryGeneralResults) {
            MenuBar.MenuItem keepResultsButton = rightBar.addItem("", new Command() {
                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(com.vaadin.ui.MenuBar.MenuItem selectedItem) {
                    queryPanel.addResultsTab(refreshWithoutSaveButton(), StringUtils.abbreviate(sql, 20),
                            queryPanel.getGeneralResultsTab().getIcon());
                    queryPanel.resetGeneralResultsTab();
                }
            });
            keepResultsButton.setIcon(VaadinIcons.COPY);
            keepResultsButton.setDescription("Save these results to a new tab");
        }

        if (showSql) {
            sqlLabel.setValue(StringUtils.abbreviate(sql, 200));
        }

        resultBar.addComponent(rightBar);
        resultBar.setComponentAlignment(rightBar, Alignment.MIDDLE_RIGHT);

        this.addComponent(resultBar, 0);
    }

    protected TabularResultLayout refreshWithoutSaveButton() {
        isInQueryGeneralResults = false;
        this.removeComponent(this.getComponent(0));
        createMenuBar();
        return this;
    }

    protected void handleAction(String action) {
        try {
            DatabaseInfo dbInfo = db.getPlatform().getDatabaseInfo();
            final String quote = db.getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? dbInfo.getDelimiterToken() : "";
            final String catalogSeparator = dbInfo.getCatalogSeparator();
            final String schemaSeparator = dbInfo.getSchemaSeparator();

            String[] columnHeaders = CommonUiUtils.getHeaderCaptions(grid);
            Set<List<Object>> selectedRowsSet = grid.getSelectedItems();
            Iterator<List<Object>> setIterator = selectedRowsSet.iterator();
            while (setIterator.hasNext()) {
                List<Object> typeValueList = new ArrayList<Object>();
                List<Object> item = setIterator.next();

                for (int i = 1; i < columnHeaders.length; i++) {
                    Object typeValue = item.get(i - 1);
                    if (typeValue instanceof String) {
                        if ("<null>".equals(typeValue) || "".equals(typeValue)) {
                            typeValue = "null";
                        } else {
                            typeValue = "'" + typeValue + "'";
                        }
                    } else if (typeValue instanceof java.util.Date) {
                        typeValue = "{ts " + "'" + FormatUtils.TIMESTAMP_FORMATTER.format(typeValue) + "'" + "}";
                    }
                    typeValueList.add(typeValue);
                }

                if (action.equals(ACTION_SELECT)) {
                    StringBuilder sql = new StringBuilder("SELECT ");

                    for (int i = 1; i < columnHeaders.length; i++) {
                        if (i == 1) {
                            sql.append(quote).append(columnHeaders[i]).append(quote);
                        } else {
                            sql.append(", ").append(quote).append(columnHeaders[i]).append(quote);
                        }
                    }

                    sql.append(" FROM " + org.jumpmind.db.model.Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote,
                            catalogSeparator, schemaSeparator));

                    sql.append(" WHERE ");

                    int track = 0;
                    for (int i = 0; i < resultTable.getColumnCount(); i++) {
                        Column col = resultTable.getColumn(i);
                        if (col.isPrimaryKey()) {
                            if (track == 0) {
                                sql.append(col.getName() + "=" + typeValueList.get(i));
                            } else {
                                sql.append(" and ").append(quote).append(col.getName()).append(quote).append("=")
                                        .append(typeValueList.get(i));
                            }
                            track++;
                        }
                    }
                    sql.append(";");
                    listener.writeSql(sql.toString());
                } else if (action.equals(ACTION_INSERT)) {
                    StringBuilder sql = new StringBuilder();
                    sql.append("INSERT INTO ").append(org.jumpmind.db.model.Table.getFullyQualifiedTableName(catalogName, schemaName,
                            tableName, quote, catalogSeparator, schemaSeparator)).append(" (");

                    for (int i = 1; i < columnHeaders.length; i++) {
                        if (i == 1) {
                            sql.append(quote + columnHeaders[i] + quote);
                        } else {
                            sql.append(", " + quote + columnHeaders[i] + quote);
                        }
                    }
                    sql.append(") VALUES (");
                    boolean first = true;
                    for (int i = 1; i < columnHeaders.length; i++) {
                        if (first) {
                            first = false;
                        } else {
                            sql.append(", ");
                        }
                        sql.append(typeValueList.get(i - 1));
                    }
                    sql.append(");");
                    listener.writeSql(sql.toString());

                } else if (action.equals(ACTION_UPDATE)) {
                    StringBuilder sql = new StringBuilder("UPDATE ");
                    sql.append(org.jumpmind.db.model.Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote,
                            catalogSeparator, schemaSeparator) + " SET ");
                    for (int i = 1; i < columnHeaders.length; i++) {
                        if (i == 1) {
                            sql.append(quote).append(columnHeaders[i]).append(quote).append("=");
                        } else {
                            sql.append(", ").append(quote).append(columnHeaders[i]).append(quote).append("=");
                        }

                        sql.append(typeValueList.get(i - 1));
                    }

                    sql.append(" WHERE ");

                    int track = 0;
                    for (int i = 0; i < resultTable.getColumnCount(); i++) {
                        Column col = resultTable.getColumn(i);
                        if (col.isPrimaryKey()) {
                            if (track == 0) {
                                sql.append(quote).append(col.getName()).append(quote).append("=").append(typeValueList.get(i));
                            } else {
                                sql.append(" and ").append(quote).append(col.getName()).append(quote).append("=")
                                        .append(typeValueList.get(i));
                            }
                            track++;
                        }
                    }
                    sql.append(";");
                    listener.writeSql(sql.toString());

                } else if (action.equals(ACTION_DELETE)) {
                    StringBuilder sql = new StringBuilder("DELETE FROM ");
                    sql.append(org.jumpmind.db.model.Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote,
                            catalogSeparator, schemaSeparator)).append(" WHERE ");
                    int track = 0;
                    for (int i = 0; i < resultTable.getColumnCount(); i++) {
                        Column col = resultTable.getColumn(i);
                        if (col.isPrimaryKey()) {
                            if (track == 0) {
                                sql.append(quote).append(col.getName()).append(quote).append("=").append(typeValueList.get(i));
                            } else {
                                sql.append(" and ").append(quote).append(col.getName()).append(quote).append("=")
                                        .append(typeValueList.get(i));
                            }
                            track++;
                        }
                    }
                    sql.append(";");
                    listener.writeSql(sql.toString());
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            Notification.show("There are an error while attempting to perform the action.  Please check the log file for further details.");
        }
    }

    protected static String getTypeValue(String type) {
        String value = null;
        if (type.equalsIgnoreCase("CHAR")) {
            value = "''";
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            value = "''";
        } else if (type.equalsIgnoreCase("LONGVARCHAR")) {
            value = "''";
        } else if (type.equalsIgnoreCase("DATE")) {
            value = "''";
        } else if (type.equalsIgnoreCase("TIME")) {
            value = "''";
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            value = "{ts ''}";
        } else if (type.equalsIgnoreCase("CLOB")) {
            value = "''";
        } else if (type.equalsIgnoreCase("BLOB")) {
            value = "''";
        } else if (type.equalsIgnoreCase("ARRAY")) {
            value = "[]";
        } else {
            value = "";
        }
        return value;
    }

    protected void buildFollowToMenu() {
        ForeignKey[] foreignKeys = resultTable.getForeignKeys();
        for (final ForeignKey foreignKey : foreignKeys) {
            String optionTitle = foreignKey.getForeignTableName() + " (";
            for (Reference ref : foreignKey.getReferences()) {
                optionTitle += ref.getLocalColumnName() + ", ";
            }
            optionTitle = optionTitle.substring(0, optionTitle.length() - 2) + ")";
            followToMenu.addItem(optionTitle, new MenuBar.Command() {

                private static final long serialVersionUID = 1L;

                
                @Override
                public void menuSelected(MenuItem selectedItem) {
                    followTo(foreignKey);
                }
            });
        }
    }

    protected void followTo(ForeignKey foreignKey) {
        Set<List<Object>> selectedRows = grid.getSelectedItems();
        if (selectedRows.size() > 0) {
            log.info("Following foreign key to " + foreignKey.getForeignTableName());

            if (queryPanel == null) {
                if (explorer != null) {
                    queryPanel = explorer.openQueryWindow(db);
                } else {
                    log.error("Failed to find current or create new query tab");
                }
            }

            Table foreignTable = foreignKey.getForeignTable();
            if (foreignTable == null) {
                foreignTable = db.getPlatform().getTableFromCache(foreignKey.getForeignTableName(), false);
            }

            Reference[] references = foreignKey.getReferences();
            for (Reference ref : references) {
                if (ref.getForeignColumn() == null) {
                    ref.setForeignColumn(foreignTable.getColumnWithName(ref.getForeignColumnName()));
                }
            }

            String sql = createFollowSql(foreignTable, references, selectedRows.size());

            try {
                PreparedStatement ps = ((DataSource) db.getPlatform().getDataSource()).getConnection().prepareStatement(sql);
                int i = 1;
                for (List<Object> row : selectedRows) {
                    for (Reference ref : references) {
                        int colNum = 0;
                        for (Grid.Column<List<Object>, ?> col : grid.getColumns()) {
                            if (col.getCaption().equals(ref.getLocalColumnName())) {
                                break;
                            }
                        }
                        int targetType = ref.getForeignColumn().getMappedTypeCode();
                        ps.setObject(i, row.get(colNum - 1), targetType);
                        i++;
                    }
                }
                sql = ps.toString().substring(ps.toString().indexOf("select "));
                queryPanel.executeSql(sql, false);
            } catch (SQLException e) {
                log.error("Failed to follow foreign key", e);
            }
        }
    }

    protected String createFollowSql(Table foreignTable, Reference[] references, int selectedRowCount) {
        DatabaseInfo dbInfo = db.getPlatform().getDatabaseInfo();
        String quote = db.getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? dbInfo.getDelimiterToken() : "";

        StringBuilder sql = new StringBuilder("select ");
        for (Column col : foreignTable.getColumns()) {
            sql.append(quote);
            sql.append(col.getName());
            sql.append(quote);
            sql.append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(" from ");
        sql.append(foreignTable.getQualifiedTableName(quote, dbInfo.getCatalogSeparator(), dbInfo.getSchemaSeparator()));
        sql.append(" where ");

        StringBuilder whereClause = new StringBuilder("(");
        for (Reference ref : references) {
            whereClause.append(ref.getForeignColumnName());
            whereClause.append("=? and ");
        }
        whereClause.delete(whereClause.length() - 5, whereClause.length());
        whereClause.append(") or ");

        for (int i = 0; i < selectedRowCount; i++) {
            sql.append(whereClause.toString());
        }
        sql.delete(sql.length() - 4, sql.length());

        return sql.toString();
    }

    protected Grid<List<Object>> putResultsInGrid(int maxResultSize) throws SQLException {
        String parsedSql = sql;
        String first = "";
        String second = "";
        String third = "";
        parsedSql = parsedSql.substring(parsedSql.toUpperCase().indexOf("FROM ") + 5, parsedSql.length());
        parsedSql = parsedSql.trim();
        String separator = ".";
        if (parsedSql.contains(separator)) {
            first = parsedSql.substring(0, parsedSql.indexOf(separator) + separator.length() - 1);
            parsedSql = parsedSql.substring(parsedSql.indexOf(separator) + separator.length(), parsedSql.length());
            if (parsedSql.contains(separator)) {
                second = parsedSql.substring(0, parsedSql.indexOf(separator) + separator.length() - 1);
                parsedSql = parsedSql.substring(parsedSql.indexOf(separator) + separator.length(), parsedSql.length());
                if (parsedSql.contains(separator)) {
                    third = parsedSql.substring(0, parsedSql.indexOf(separator) + separator.length() - 1);
                    parsedSql = parsedSql.substring(parsedSql.indexOf(separator) + separator.length(), parsedSql.length());
                } else {
                    third = parsedSql;
                }
            } else {
                second = parsedSql;
            }
        } else {
            first = parsedSql;
        }
        if (!third.equals("")) {
            tableName = third;
            schemaName = second;
            catalogName = first;
        } else if (!second.equals("")) {
            if (db.getPlatform().getDefaultCatalog() != null) {
                IDdlReader reader = db.getPlatform().getDdlReader();
                List<String> catalogs = reader.getCatalogNames();
                if (catalogs.contains(first)) {
                    catalogName = first;
                } else if (db.getPlatform().getDefaultSchema() != null) {
                    Iterator<String> iterator = catalogs.iterator();
                    while (iterator.hasNext()) {
                        List<String> schemas = reader.getSchemaNames(iterator.next());
                        if (schemas.contains(first)) {
                            schemaName = first;
                        }
                    }
                }
            } else if (db.getPlatform().getDefaultSchema() != null) {
                schemaName = first;
            }
            tableName = second;
        } else if (!first.equals("")) {
            tableName = parsedSql;
        }

        if (isNotBlank(tableName)) {
            if (tableName.contains(" ")) {
                tableName = tableName.substring(0, tableName.indexOf(" "));
            }
            if (isBlank(schemaName)) {
                schemaName = null;
            }
            if (isBlank(catalogName)) {
                catalogName = null;
            }
            String quote = "\"";
            if (catalogName != null && catalogName.contains(quote)) {
                catalogName = catalogName.replaceAll(quote, "");
                catalogName = catalogName.trim();
            }
            if (schemaName != null && schemaName.contains(quote)) {
                schemaName = schemaName.replaceAll(quote, "");
                schemaName = schemaName.trim();
            }
            if (tableName != null && tableName.contains(quote)) {
                tableName = tableName.replaceAll(quote, "");
                tableName = tableName.trim();
            }
            try {
                resultTable = db.getPlatform().getTableFromCache(catalogName, schemaName, tableName, false);
                if (resultTable != null) {
                    tableName = resultTable.getName();
                    if (isNotBlank(catalogName) && isNotBlank(resultTable.getCatalog())) {
                        catalogName = resultTable.getCatalog();
                    }
                    if (isNotBlank(schemaName) && isNotBlank(resultTable.getSchema())) {
                        schemaName = resultTable.getSchema();
                    }

                }
            } catch (Exception e) {
                log.debug("Failed to lookup table: " + tableName, e);
            }
        }

        TypedProperties properties = settings.getProperties();
        return CommonUiUtils.putResultsInGrid(rs, properties.getInt(SQL_EXPLORER_MAX_RESULTS),
                properties.is(SQL_EXPLORER_SHOW_ROW_NUMBERS), getColumnsToExclude());

    }

    protected String[] getColumnsToExclude() {
        return new String[0];
    }
}
