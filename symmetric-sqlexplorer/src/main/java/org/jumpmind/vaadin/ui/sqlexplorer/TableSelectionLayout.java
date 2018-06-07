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

import static org.apache.commons.lang.StringUtils.containsIgnoreCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.UiConstants;

import com.vaadin.server.FontAwesome;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;
import com.vaadin.v7.data.Item;
import com.vaadin.v7.data.Property;
import com.vaadin.v7.data.Property.ValueChangeEvent;
import com.vaadin.v7.event.FieldEvents.TextChangeEvent;
import com.vaadin.v7.event.FieldEvents.TextChangeListener;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.v7.ui.AbstractSelect;
import com.vaadin.v7.ui.AbstractTextField.TextChangeEventMode;
import com.vaadin.v7.ui.CheckBox;
import com.vaadin.v7.ui.ComboBox;
import com.vaadin.v7.ui.Table;
import com.vaadin.v7.ui.TextField;

public class TableSelectionLayout extends VerticalLayout {

    private Set<org.jumpmind.db.model.Table> selectedTablesSet;

    private static final long serialVersionUID = 1L;

    public Table listOfTablesTable;

    public AbstractSelect catalogSelect;

    public AbstractSelect schemaSelect;

    @SuppressWarnings("unused")
    private String filterCriteria = null;

    private TextField filterField;

    private IDatabasePlatform databasePlatform;
    
    private List<String> excludedTables;

    public TableSelectionLayout(IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet) {
        this("Please select from the following tables", databasePlatform, selectedSet, null);
    }
    
    public TableSelectionLayout(String titleKey, IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet) {
    	this(titleKey, databasePlatform, selectedSet, null);
    }

    public TableSelectionLayout(String titleKey, IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet, List<String> excludedTables) {
        super();
        this.setSizeFull();
        this.setMargin(true);
        this.setSpacing(true);

        this.selectedTablesSet = selectedSet;
        this.databasePlatform = databasePlatform;
        this.excludedTables = excludedTables;

        createTableSelectionLayout(titleKey);
    }

    protected void createTableSelectionLayout(String titleKey) {

        this.addComponent(new Label(titleKey));

        HorizontalLayout schemaChooserLayout = new HorizontalLayout();
        schemaChooserLayout.setWidth(100, Unit.PERCENTAGE);
        schemaChooserLayout.setSpacing(true);
        this.addComponent(schemaChooserLayout);

        catalogSelect = new ComboBox("Catalog");
        catalogSelect.setImmediate(true);
        CommonUiUtils.addItems(getCatalogs(), catalogSelect);
        schemaChooserLayout.addComponent(catalogSelect);
        if (selectedTablesSet.iterator().hasNext()) {
            catalogSelect.select(selectedTablesSet.iterator().next().getCatalog());
        } else {
            catalogSelect.select(databasePlatform.getDefaultCatalog());
        }
        schemaSelect = new ComboBox("Schema");
        schemaSelect.setImmediate(true);
        CommonUiUtils.addItems(getSchemas(), schemaSelect);
        schemaChooserLayout.addComponent(schemaSelect);
        if (selectedTablesSet.iterator().hasNext()) {
            schemaSelect.select(selectedTablesSet.iterator().next().getSchema());
        } else {
            schemaSelect.select(databasePlatform.getDefaultSchema());
        }

        Label spacer = new Label();
        schemaChooserLayout.addComponent(spacer);
        schemaChooserLayout.setExpandRatio(spacer, 1);

        filterField = new TextField();
        filterField.addStyleName(ValoTheme.TEXTFIELD_INLINE_ICON);
        filterField.setIcon(FontAwesome.SEARCH);
        filterField.setInputPrompt("Filter Tables");
        filterField.setNullRepresentation("");
        filterField.setImmediate(true);
        filterField.setTextChangeEventMode(TextChangeEventMode.LAZY);
        filterField.setTextChangeTimeout(200);
        filterField.addTextChangeListener(new TextChangeListener() {
            private static final long serialVersionUID = 1L;

            public void textChange(TextChangeEvent event) {
                filterField.setValue(event.getText());
                refreshTableOfTables();
            }
        });

        schemaChooserLayout.addComponent(filterField);
        schemaChooserLayout.setComponentAlignment(filterField, Alignment.BOTTOM_RIGHT);

        listOfTablesTable = CommonUiUtils.createTable();
        listOfTablesTable.setImmediate(true);
        listOfTablesTable.addItemClickListener(new ItemClickListener() {            
            private static final long serialVersionUID = 1L;
            @Override
            public void itemClick(ItemClickEvent event) {
                CheckBox checkBox = (CheckBox)event.getItem().getItemProperty("selected").getValue();
                checkBox.setValue(!checkBox.getValue());
            }
        });
        listOfTablesTable.addContainerProperty("selected", CheckBox.class, null);
        listOfTablesTable.setColumnWidth("selected", UiConstants.TABLE_SELECTED_COLUMN_WIDTH);
        listOfTablesTable.setColumnHeader("selected", "");
        listOfTablesTable.addContainerProperty("table", String.class, null);
        listOfTablesTable.setColumnHeader("table", "");
        listOfTablesTable.setSizeFull();
        this.addComponent(listOfTablesTable);
        this.setExpandRatio(listOfTablesTable, 1);

        schemaSelect.addValueChangeListener(new Property.ValueChangeListener() {
            private static final long serialVersionUID = 1L;

            public void valueChange(ValueChangeEvent event) {
                refreshTableOfTables();
            }
        });

        catalogSelect.addValueChangeListener(new Property.ValueChangeListener() {
            private static final long serialVersionUID = 1L;

            public void valueChange(ValueChangeEvent event) {
                refreshTableOfTables();
            }
        });

        Button selectAllLink = new Button("Select All");
        selectAllLink.addStyleName(ValoTheme.BUTTON_LINK);
        selectAllLink.addClickListener((event) -> selectAll());

        Button selectNoneLink = new Button("Select None");
        selectNoneLink.addStyleName(ValoTheme.BUTTON_LINK);
        selectNoneLink.addClickListener((event) -> selectNone());
        
        HorizontalLayout selectAllFooter = new HorizontalLayout();

        selectAllFooter.setWidth("100%");
        selectAllFooter.setSpacing(true);
        selectAllFooter.addStyleName(ValoTheme.WINDOW_BOTTOM_TOOLBAR);

        selectAllFooter.addComponent(selectAllLink);
        selectAllFooter.addComponent(selectNoneLink);
        
        this.addComponent(selectAllFooter);
        
        refreshTableOfTables();

    }

    public String getSelectedSchema() {
        String schemaName = (String) schemaSelect.getValue();
        if (schemaName != null && schemaName.equals(databasePlatform.getDefaultSchema())) {
            schemaName = null;
        }
        return StringUtils.isBlank(schemaName) ? null : schemaName;
    }

    public String getSelectedCatalog() {
        String catalogName = (String) catalogSelect.getValue();
        if (catalogName != null && catalogName.equals(databasePlatform.getDefaultCatalog())) {
            catalogName = null;
        }
        return StringUtils.isBlank(catalogName) ? null : catalogName;
    }

    protected void refreshTableOfTables() {
        listOfTablesTable.removeAllItems();
        List<String> tables = getTables();
        String filter = filterField.getValue();

        for (String table : tables) {
            if ((excludedTables == null || !excludedTables.contains(table.toLowerCase())) && display(getSelectedCatalog(), getSelectedSchema(), table)) {
                if (!filter.equals("")) {
                    if (containsIgnoreCase(table, filter)) {
                        populateTable(table);
                    }
                } else {
                    populateTable(table);
                }
            }
        }
    }

    private void populateTable(final String table) {
        final CheckBox checkBox = new CheckBox();
        checkBox.setValue(select(getSelectedCatalog(), getSelectedSchema(), table));
        listOfTablesTable.addItem(new Object[] { checkBox, table }, table);
        checkBox.addValueChangeListener(new Property.ValueChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void valueChange(ValueChangeEvent event) {
                if (checkBox.getValue()) {
                    org.jumpmind.db.model.Table t = new org.jumpmind.db.model.Table(table);
                    selectedTablesSet.add(t);
                } else {
                    Iterator<org.jumpmind.db.model.Table> selectedIterator = selectedTablesSet
                            .iterator();
                    boolean notFound = true;
                    while (selectedIterator.hasNext() || notFound) {
                        if (selectedIterator.next().getName().equals(table)) {
                            selectedIterator.remove();
                            notFound = false;
                        }
                    }
                }
                selectionChanged();
            }
        });
    }

    protected void selectionChanged() {

    }

    @SuppressWarnings("unchecked")
    public List<String> getSelectedTables() {
        listOfTablesTable.commit();
        List<String> select = new ArrayList<String>();
        Collection<Object> itemIds = (Collection<Object>) listOfTablesTable.getItemIds();
        for (Object itemId : itemIds) {
            Item item = listOfTablesTable.getItem(itemId);
            CheckBox checkBox = (CheckBox) item.getItemProperty("selected").getValue();
            if (checkBox.getValue().equals(Boolean.TRUE) && checkBox.isEnabled()) {
                select.add((String) itemId);
            }
        }
        return select;
    }

    public void selectAll() {
        @SuppressWarnings("unchecked")
        Collection<Object> itemIds = (Collection<Object>) listOfTablesTable.getItemIds();
        for (Object itemId : itemIds) {
            Item item = listOfTablesTable.getItem(itemId);
            CheckBox checkBox = (CheckBox) item.getItemProperty("selected").getValue();
            if (checkBox.isEnabled()) {
                checkBox.setValue(Boolean.TRUE);
            }
        }
    }

    public void selectNone() {
        @SuppressWarnings("unchecked")
        Collection<Object> itemIds = (Collection<Object>) listOfTablesTable.getItemIds();
        for (Object itemId : itemIds) {
            Item item = listOfTablesTable.getItem(itemId);
            CheckBox checkBox = (CheckBox) item.getItemProperty("selected").getValue();
            if (checkBox.isEnabled()) {
                checkBox.setValue(Boolean.FALSE);
            }
        }
    }

    public List<String> getSchemas() {
        return databasePlatform.getDdlReader().getSchemaNames(null);
    }

    public List<String> getCatalogs() {
        return databasePlatform.getDdlReader().getCatalogNames();
    }

    public List<String> getTables() {
        return databasePlatform.getDdlReader().getTableNames((String) catalogSelect.getValue(),
                (String) schemaSelect.getValue(), new String[] { "TABLE" });
    }
    
    public List<String> getExcludedTables() {
    	return excludedTables;
    }
    
    public void setExcludedTables(List<String> excludedTables) {
    	this.excludedTables = excludedTables;
    }

    protected boolean display(String catalog, String schema, String table) {
        return true;
    }

    protected boolean select(String catalog, String schema, String table) {
        for (org.jumpmind.db.model.Table t : this.selectedTablesSet) {
            if (table.equals(t.getName())) {
                return true;
            }
        }
        return false;
    }

}
