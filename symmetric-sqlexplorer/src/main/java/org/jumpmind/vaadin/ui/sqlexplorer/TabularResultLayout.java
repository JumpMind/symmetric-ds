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

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_MAX_RESULTS;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_SHOW_ROW_NUMBERS;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ExportDialog;
import org.jumpmind.vaadin.ui.common.Grid7DataProvider;
import org.jumpmind.vaadin.ui.common.NotifyDialog;
import org.jumpmind.vaadin.ui.common.ReadOnlyTextAreaDialog;
import org.jumpmind.vaadin.ui.sqlexplorer.SqlRunner.ISqlRunnerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.contextmenu.ContextMenu;
import com.vaadin.contextmenu.MenuItem;
import com.vaadin.server.FontAwesome;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.Property;
import com.vaadin.v7.data.Validator;
import com.vaadin.v7.data.fieldgroup.FieldGroup.CommitEvent;
import com.vaadin.v7.data.fieldgroup.FieldGroup.CommitException;
import com.vaadin.v7.data.fieldgroup.FieldGroup.CommitHandler;
import com.vaadin.v7.data.util.converter.Converter;
import com.vaadin.v7.data.util.converter.StringToBigDecimalConverter;
import com.vaadin.v7.data.util.converter.StringToBooleanConverter;
import com.vaadin.v7.data.util.converter.StringToLongConverter;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.shared.ui.label.ContentMode;
import com.vaadin.v7.ui.CustomField;
import com.vaadin.v7.ui.Grid;
import com.vaadin.v7.ui.Grid.CellReference;
import com.vaadin.v7.ui.Grid.CellStyleGenerator;
import com.vaadin.v7.ui.Label;
import com.vaadin.v7.ui.TextField;

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

    Grid grid;

    org.jumpmind.db.model.Table resultTable;

    String sql;

    ResultSet rs;

    IDb db;

    ISqlRunnerListener listener;

    String user;

    Settings settings;

    boolean showSql = true;

    boolean isInQueryGeneralResults;

    MenuItem followToMenu;

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

            initGridEditing();

            grid.setCellStyleGenerator(new CellStyleGenerator() {
                private static final long serialVersionUID = 1L;

                @Override
                public String getStyle(CellReference cell) {
                    if (cell.getPropertyId().equals("#") && !grid.getSelectedRows().contains(cell.getItemId())) {
                        return "rowheader";
                    }
                    if (cell.getValue() == null) {
                        return "italics";
                    }
                    return null;
                }
            });

            ContextMenu menu = new ContextMenu(grid, true);
            menu.addItem(ACTION_SELECT, new ContextMenu.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(com.vaadin.contextmenu.MenuItem selectedItem) {
                    handleAction(ACTION_SELECT);
                }
            });
            menu.addItem(ACTION_INSERT, new ContextMenu.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(com.vaadin.contextmenu.MenuItem selectedItem) {
                    handleAction(ACTION_INSERT);
                }
            });
            menu.addItem(ACTION_UPDATE, new ContextMenu.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(com.vaadin.contextmenu.MenuItem selectedItem) {
                    handleAction(ACTION_UPDATE);
                }
            });
            menu.addItem(ACTION_DELETE, new ContextMenu.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(com.vaadin.contextmenu.MenuItem selectedItem) {
                    handleAction(ACTION_DELETE);
                }
            });

            if (resultTable != null && resultTable.getForeignKeyCount() > 0) {
                followToMenu = menu.addItem("Follow to", null);
                buildFollowToMenu();
            }

            grid.addItemClickListener(new ItemClickListener() {

                private static final long serialVersionUID = 1L;

                @Override
                public void itemClick(ItemClickEvent event) {
                    MouseButton button = event.getButton();
                    if (button == MouseButton.LEFT) {
                        Object object = event.getPropertyId();
                        if (object != null && !object.toString().equals("")) {
                            if (event.isDoubleClick() && !grid.isEditorEnabled()) {
                                Object prop = event.getPropertyId();
                                String header = grid.getColumn(prop).getHeaderCaption();
                                Property<?> p = event.getItem().getItemProperty(prop);
                                if (p != null) {
                                    String data = String.valueOf(p.getValue());
                                    boolean binary = resultTable != null ? resultTable.getColumnWithName(header).isOfBinaryType() : false;
                                    if (binary) {
                                        ReadOnlyTextAreaDialog.show(header, data.toUpperCase(), binary);
                                    } else {
                                        ReadOnlyTextAreaDialog.show(header, data, binary);
                                    }
                                }
                            } else {
                                Object row = event.getItemId();
                                if (!grid.getSelectedRows().contains(row)) {
                                    grid.deselectAll();
                                    grid.select(row);
                                } else {
                                    grid.deselect(row);
                                }
                            }
                        }
                    }
                }
            });
            
            this.addComponent(grid);
            this.setExpandRatio(grid, 1);

            int count = (grid.getContainerDataSource().getItemIds().size());
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
        refreshButton.setIcon(FontAwesome.REFRESH);
        refreshButton.setDescription("Refresh");
        
        MenuBar.MenuItem exportButton = rightBar.addItem("", new Command() {
            private static final long serialVersionUID = 1L;
            
            @Override
            public void menuSelected(MenuBar.MenuItem selectedItem) {
                new ExportDialog(new Grid7DataProvider(grid), db.getName(), sql).show();
            }
        });
        exportButton.setIcon(FontAwesome.UPLOAD);
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
            keepResultsButton.setIcon(FontAwesome.CLONE);
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
            Collection<Object> selectedRowsSet = grid.getSelectedRows();
            Iterator<Object> setIterator = selectedRowsSet.iterator();
            while (setIterator.hasNext()) {
                List<Object> typeValueList = new ArrayList<Object>();
                int row = (Integer) setIterator.next();
                Item item = grid.getContainerDataSource().getItem(row);
                Iterator<?> iterator = item.getItemPropertyIds().iterator();
                iterator.next();

                for (int i = 1; i < columnHeaders.length; i++) {
                    Object typeValue = item.getItemProperty(iterator.next()).getValue();
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
            followToMenu.addItem(optionTitle, new ContextMenu.Command() {

                private static final long serialVersionUID = 1L;

                @Override
                public void menuSelected(MenuItem selectedItem) {
                    followTo(foreignKey);
                }
            });
        }
    }

    protected void followTo(ForeignKey foreignKey) {
        Collection<Object> selectedRows = grid.getSelectedRows();
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
                for (Object row : selectedRows) {
                    for (Reference ref : references) {
                        Object targetObject = grid.getContainerDataSource().getItem(row).getItemProperty(ref.getLocalColumnName()).getValue();
                        int targetType = ref.getForeignColumn().getMappedTypeCode();
                        ps.setObject(i, targetObject, targetType);
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

    protected Grid putResultsInGrid(int maxResultSize) throws SQLException {
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
        return CommonUiUtils.putResultsInGrid(rs, resultTable, properties.getInt(SQL_EXPLORER_MAX_RESULTS),
                properties.is(SQL_EXPLORER_SHOW_ROW_NUMBERS), getColumnsToExclude());

    }

    protected String[] getColumnsToExclude() {
        return new String[0];
    }

    private void initGridEditing() {
        if (resultTable != null) {
            grid.setEditorEnabled(true);
            List<com.vaadin.v7.ui.Grid.Column> columns = grid.getColumns();
            List<TextField> primaryKeyEditors = new ArrayList<TextField>();
            for (com.vaadin.v7.ui.Grid.Column gridColumn : columns) {
                String header = gridColumn.getHeaderCaption();
                Column tableColumn = resultTable.getColumnWithName(header);
                if (columns.get(0).equals(gridColumn) || (tableColumn != null && tableColumn.isAutoIncrement()
                        && !db.getPlatform().getDatabaseInfo().isAutoIncrementUpdateAllowed())) {
                    gridColumn.setEditable(false);
                } else if (tableColumn != null && db.getPlatform().isLob(tableColumn.getMappedTypeCode())) {
                    gridColumn.setEditorField(new LobEditorField(header));
                } else if (tableColumn != null) {
                    setEditor(gridColumn, tableColumn, primaryKeyEditors);
                }
            }

            for (TextField editor : primaryKeyEditors) {
                editor.addValidator(new PrimaryKeyValidator(primaryKeyEditors));
            }

            initCommit();
        } else {
            log.info("Table editing disabled.");
        }
    }

    private void setEditor(Grid.Column gridColumn, Column tableColumn, List<TextField> primaryKeyEditors) {
        TextField editor = new TextField();
        int typeCode = tableColumn.getMappedTypeCode();

        switch (typeCode) {
            case Types.DATE:
                editor.setConverter(new ObjectConverter(Date.class, typeCode));
                break;
            case Types.TIME:
                editor.setConverter(new ObjectConverter(Time.class, typeCode));
                break;
            case Types.TIMESTAMP:
                editor.setConverter(new ObjectConverter(Timestamp.class, typeCode));
                break;
            case Types.BIT:
                editor.setConverter(new StringToBooleanConverter());
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.INTEGER:
                editor.setConverter(new StringToLongConverter() {
                    private static final long serialVersionUID = 1L;

                    public NumberFormat getFormat(Locale locale) {
                        NumberFormat format = super.getFormat(locale);
                        format.setGroupingUsed(false);
                        return format;
                    }
                });
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
            case Types.NUMERIC:
            case Types.DECIMAL:
                editor.setConverter(new StringToBigDecimalConverter() {
                    private static final long serialVersionUID = 1L;

                    public NumberFormat getFormat(Locale locale) {
                        NumberFormat format = super.getFormat(locale);
                        format.setGroupingUsed(false);
                        return format;
                    }
                });
                break;
            default:
                break;
        }

        editor.addValidator(new TableChangeValidator(editor, tableColumn));

        editor.setNullRepresentation("");
        if (!tableColumn.isRequired()) {
            editor.setNullSettingAllowed(true);
        }

        if (tableColumn.isPrimaryKey()) {
            primaryKeyEditors.add(editor);
        }

        gridColumn.setEditorField(editor);
    }

    private void initCommit() {
        grid.getEditorFieldGroup().addCommitHandler(new CommitHandler() {

            private static final long serialVersionUID = 1L;

            Map<Object, Object> unchangedValues;
            Object[] params;
            int[] types;

            @Override
            public void preCommit(CommitEvent commitEvent) throws CommitException {
                Item row = commitEvent.getFieldBinder().getItemDataSource();
                unchangedValues = new HashMap<Object, Object>();
                params = new Object[resultTable.getPrimaryKeyColumnCount() + 1];
                types = new int[params.length];
                int paramCount = 1;
                for (Object id : row.getItemPropertyIds()) {
                    unchangedValues.put(id, row.getItemProperty(id).getValue());
                    if (resultTable.getPrimaryKeyColumnIndex(id.toString()) >= 0) {
                        params[paramCount] = commitEvent.getFieldBinder().getItemDataSource().getItemProperty(id).getValue();
                        types[paramCount] = resultTable.getColumnWithName(id.toString()).getMappedTypeCode();
                        paramCount++;
                    }
                }
            }

            @Override
            public void postCommit(CommitEvent commitEvent) throws CommitException {
                Item row = commitEvent.getFieldBinder().getItemDataSource();
                for (Object id : row.getItemPropertyIds()) {
                    if (grid.getColumn(id).isEditable()
                            && !db.getPlatform().isLob(resultTable.getColumnWithName(id.toString()).getMappedTypeCode())) {
                        String sql = buildUpdate(resultTable, id.toString(), resultTable.getPrimaryKeyColumnNames());
                        params[0] = row.getItemProperty(id).getValue();
                        if ((params[0] == null && unchangedValues.get(id) == null)
                                || (params[0] != null && params[0].equals(unchangedValues.get(id)))) {
                            continue;
                        }
                        types[0] = resultTable.getColumnWithName(id.toString()).getMappedTypeCode();
                        for (int i = 0; i < types.length; i++) {
                            if (types[i] == Types.DATE && db.getPlatform().getDdlBuilder().getDatabaseInfo().isDateOverridesToTimestamp()) {
                                types[i] = Types.TIMESTAMP;
                            }
                        }
                        try {
                            db.getPlatform().getSqlTemplate().update(sql, params, types);
                        } catch (SqlException e) {
                            NotifyDialog.show("Error",
                                    "<b>The table could not be updated.</b><br>"
                                            + "Cause: the sql update statement failed to execute.<br><br>"
                                            + "To view the <b>Stack Trace</b>, click <b>\"Details\"</b>.",
                                    e, Type.ERROR_MESSAGE);
                        }
                    }
                }
                listener.reExecute(sql);
            }
        });
    }

    protected Object[] getPrimaryKeys() {
        String[] columnNames = resultTable.getPrimaryKeyColumnNames();
        Object[] pks = new Object[columnNames.length];
        Item row = grid.getContainerDataSource().getItem(grid.getEditedItemId());
        int index = 0;
        for (String columnName : columnNames) {
            Property<?> p = row.getItemProperty(columnName);
            if (p != null) {
                pks[index++] = p.getValue();
            } else {
                return null;
            }
        }
        return pks;
    }

    protected String buildUpdate(Table table, String columnName, String[] pkColumnNames) {
        StringBuilder sql = new StringBuilder("update ");
        DatabaseInfo dbInfo = db.getPlatform().getDatabaseInfo();
        String quote = db.getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? dbInfo.getDelimiterToken() : "";
        sql.append(table.getQualifiedTableName(quote, dbInfo.getCatalogSeparator(), dbInfo.getSchemaSeparator()));
        sql.append(" set ");
        sql.append(quote);
        sql.append(columnName);
        sql.append(quote);
        sql.append("=? where ");
        for (String col : pkColumnNames) {
            sql.append(quote);
            sql.append(col);
            sql.append(quote);
            sql.append("=? and ");
        }
        sql.delete(sql.length() - 5, sql.length());
        return sql.toString();
    }

    class LobEditorField extends CustomField<String> {

        private static final long serialVersionUID = 1L;

        String header;

        LobEditorField(String header) {
            super();
            this.header = header;
        }

        @Override
        protected Component initContent() {
            final Button button = new Button("...");
            button.addClickListener(new ClickListener() {

                private static final long serialVersionUID = 1L;

                public void buttonClick(ClickEvent event) {
                    Property<?> p = grid.getContainerDataSource().getItem(grid.getEditedItemId()).getItemProperty(header);
                    if (p != null) {
                        String data = p.getValue() == null ? null : String.valueOf(p.getValue());
                        boolean binary = resultTable != null ? resultTable.getColumnWithName(header).isOfBinaryType() : false;
                        if (binary) {
                            ReadOnlyTextAreaDialog.show(header, data == null ? null : data.toUpperCase(), resultTable, getPrimaryKeys(),
                                    db.getPlatform(), binary, true);
                        } else {
                            ReadOnlyTextAreaDialog.show(header, data, resultTable, getPrimaryKeys(), db.getPlatform(), binary, true);
                        }
                    }
                }
            });
            return button;
        }

        @Override
        public Class<? extends String> getType() {
            return String.class;
        }

    }

    class ObjectConverter implements Converter<String, Object> {

        private static final long serialVersionUID = 1L;

        Class<?> modelType;
        int typeCode;

        ObjectConverter(Class<?> modelType, int typeCode) {
            super();
            this.modelType = modelType;
            this.typeCode = typeCode;
        }

        @Override
        public Object convertToModel(String value, Class<? extends Object> targetType, Locale locale)
                throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
            if (value == null || value.isEmpty() || value.equals("<null>")) {
                return null;
            }

            if (java.util.Date.class.isAssignableFrom(modelType)) {
                try {
                    return modelType.cast(db.getPlatform().parseDate(typeCode, value, false));
                } catch (Exception e) {
                    return value;
                }
            }

            return value.toString();
        }

        @Override
        public String convertToPresentation(Object value, Class<? extends String> targetType, Locale locale)
                throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
            if (value == null || value.equals("") || value.equals("<null>"))
                return "";
            return String.valueOf(value);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public Class getModelType() {
            if (typeCode == Types.DATE && db.getPlatform().getDdlBuilder().getDatabaseInfo().isDateOverridesToTimestamp()) {
                modelType = Timestamp.class;
            }
            return modelType;
        }

        @Override
        public Class<String> getPresentationType() {
            return String.class;
        }
    }

    class TableChangeValidator implements Validator {

        private static final long serialVersionUID = 1L;

        TextField editor;
        Column col;

        TableChangeValidator(TextField editor, Column col) {
            super();
            this.editor = editor;
            this.col = col;
        }

        @Override
        public void validate(Object value) throws InvalidValueException {
            if (value == null || value.toString().isEmpty()) {
                if (col.isRequired()) {
                    throw new EmptyValueException("Value cannot be null");
                }
            } else if (editor.getConverter() instanceof ObjectConverter) {
                int typeCode = col.getMappedTypeCode();
                if (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP) {
                    try {
                        db.getPlatform().parseDate(typeCode, String.valueOf(value), false);
                    } catch (Exception e) {
                        throw new InvalidValueException(col.getMappedType() + " format not valid");
                    }
                }
            }
        }
    }

    class PrimaryKeyValidator implements Validator {

        private static final long serialVersionUID = 1L;

        List<TextField> editors;

        PrimaryKeyValidator(List<TextField> editors) {
            super();
            this.editors = editors;
        }

        public void validate(Object value) throws InvalidValueException {
            String[] pkColumns = resultTable.getPrimaryKeyColumnNames();
            if (editors.size() != pkColumns.length) {
                throw new IllegalArgumentException();
            }
            Object[] newValues = new Object[pkColumns.length];
            for (int i = 0; i < editors.size(); i++) {
                TextField editor = editors.get(i);
                if (editor.getConverter() != null) {
                    newValues[i] = editor.getConverter().convertToModel(editor.getValue(), editor.getConverter().getModelType(),
                            editor.getLocale());
                } else {
                    newValues[i] = editor.getValue();
                }
            }
            allColumns: for (Object row : grid.getContainerDataSource().getItemIds()) {
                if (!row.equals(grid.getEditedItemId())) {
                    for (int i = 0; i < pkColumns.length; i++) {
                        if (!grid.getContainerDataSource().getItem(row).getItemProperty(pkColumns[i]).getValue().equals(newValues[i])) {
                            continue allColumns;
                        }
                    }
                    throw new InvalidValueException("Cannot use repeated primary keys");
                }
            }
        }
    }
}
