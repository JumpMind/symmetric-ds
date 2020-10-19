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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import com.vaadin.data.provider.Query;
import com.vaadin.icons.VaadinIcons;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Grid.SelectionMode;
import com.vaadin.ui.themes.ValoTheme;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Grid;
import com.vaadin.ui.TextField;

public class TableSelectionLayout extends VerticalLayout {

    private Set<org.jumpmind.db.model.Table> selectedTablesSet;
    
    private final Set<org.jumpmind.db.model.Table> originalSelectedTablesSet;

    private static final long serialVersionUID = 1L;
    
    public Grid<String> listOfTablesGrid;

    public ComboBox<String> catalogSelect;

    public ComboBox<String> schemaSelect;

    @SuppressWarnings("unused")
    private String filterCriteria = null;

    private TextField filterField;

    private IDatabasePlatform databasePlatform;
    
    private List<String> excludedTables;

    private String excludeTablesRegex;
    
    public TableSelectionLayout(IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet, String excludeTablesRegex) {
        this("Please select from the following tables", databasePlatform, selectedSet, null, excludeTablesRegex);
    }
    
    public TableSelectionLayout(String titleKey, IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet) {
        this(titleKey, databasePlatform, selectedSet, null, null);
    }

    public TableSelectionLayout(String titleKey, IDatabasePlatform databasePlatform,
            Set<org.jumpmind.db.model.Table> selectedSet, List<String> excludedTables, String excludeTablesRegex) {
        super();
        this.setSizeFull();
        this.setMargin(true);
        this.setSpacing(true);

        this.selectedTablesSet = selectedSet;
        this.originalSelectedTablesSet = selectedSet;
        this.databasePlatform = databasePlatform;
        this.excludedTables = excludedTables;
        this.excludeTablesRegex = excludeTablesRegex;

        createTableSelectionLayout(titleKey);
    }

    protected void createTableSelectionLayout(String titleKey) {

        this.addComponent(new Label(titleKey));

        HorizontalLayout schemaChooserLayout = new HorizontalLayout();
        schemaChooserLayout.setWidth(100, Unit.PERCENTAGE);
        schemaChooserLayout.setSpacing(true);
        this.addComponent(schemaChooserLayout);

        catalogSelect = new ComboBox<String>("Catalog", getCatalogs());
        schemaChooserLayout.addComponent(catalogSelect);
        if (selectedTablesSet.iterator().hasNext()) {
            catalogSelect.setSelectedItem(selectedTablesSet.iterator().next().getCatalog());
        } else {
            catalogSelect.setSelectedItem(databasePlatform.getDefaultCatalog());
        }
        schemaSelect = new ComboBox<String>("Schema", getSchemas());
        schemaChooserLayout.addComponent(schemaSelect);
        if (selectedTablesSet.iterator().hasNext()) {
            schemaSelect.setSelectedItem(selectedTablesSet.iterator().next().getSchema());
        } else {
            schemaSelect.setSelectedItem(databasePlatform.getDefaultSchema());
        }

        Label spacer = new Label();
        schemaChooserLayout.addComponent(spacer);
        schemaChooserLayout.setExpandRatio(spacer, 1);

        filterField = new TextField();
        filterField.addStyleName(ValoTheme.TEXTFIELD_INLINE_ICON);
        filterField.setIcon(VaadinIcons.SEARCH);
        filterField.setPlaceholder("Filter Tables");
        filterField.setValueChangeTimeout(200);
        filterField.addValueChangeListener(event -> {
            filterField.setValue(event.getValue());
            refreshTableOfTables();
        });

        schemaChooserLayout.addComponent(filterField);
        schemaChooserLayout.setComponentAlignment(filterField, Alignment.BOTTOM_RIGHT);
        
        listOfTablesGrid = new Grid<String>();
        listOfTablesGrid.setSizeFull();
        listOfTablesGrid.setSelectionMode(SelectionMode.MULTI);
        listOfTablesGrid.addItemClickListener(event -> {
           listOfTablesGrid.deselectAll();
           listOfTablesGrid.select(event.getItem());
        });
        listOfTablesGrid.addSelectionListener(event -> {
            selectedTablesSet.clear();
            selectedTablesSet.addAll(originalSelectedTablesSet);
            for (String table : listOfTablesGrid.getSelectedItems()){
                selectedTablesSet.add(new org.jumpmind.db.model.Table(table));
            }
            selectionChanged();
        });
        
        listOfTablesGrid.addColumn(table -> table);
        
        this.addComponent(listOfTablesGrid);
        this.setExpandRatio(listOfTablesGrid, 1);

        schemaSelect.addValueChangeListener(event -> refreshTableOfTables());

        catalogSelect.addValueChangeListener(event -> refreshTableOfTables());

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
        List<String> tables = getTables();
        String filter = filterField.getValue();
        List<String> filteredTables = new ArrayList<String>();

        for (String table : tables) {
            if ((excludedTables == null || !excludedTables.contains(table.toLowerCase())) && display(getSelectedCatalog(), getSelectedSchema(), table)) {
                if (!filter.equals("")) {
                    if (containsIgnoreCase(table, filter)) {
                        filteredTables.add(table);
                    }
                } else {
                    filteredTables.add(table);
                }
            }
        }
        listOfTablesGrid.setItems(filteredTables);
    }

    protected void selectionChanged() {

    }

    public List<String> getSelectedTables() {
        return new ArrayList<String>(listOfTablesGrid.getSelectedItems());
    }

    public void selectAll() {
        for (String table : listOfTablesGrid.getDataProvider().fetch(new Query<>()).collect(Collectors.toList())) {
            listOfTablesGrid.select(table);
        }
        selectionChanged();
    }

    public void selectNone() {
        listOfTablesGrid.deselectAll();
        selectionChanged();
    }

    public List<String> getSchemas() {
        return databasePlatform.getDdlReader().getSchemaNames(null);
    }

    public List<String> getCatalogs() {
        return databasePlatform.getDdlReader().getCatalogNames();
    }

    public List<String> getTables() {
        List<String> tableNames = databasePlatform.getDdlReader().getTableNames((String) catalogSelect.getValue(),
                (String) schemaSelect.getValue(), new String[] { "TABLE" });
        Iterator<String> iter = tableNames.iterator();
        while (iter.hasNext()) {
            String tableName = iter.next();
            if (tableName.matches(excludeTablesRegex)
                    || tableName.toUpperCase().matches(excludeTablesRegex)
                    || tableName.toLowerCase().matches(excludeTablesRegex)) {
                iter.remove();
            }
        }
        return tableNames;
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
