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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.jumpmind.vaadin.ui.common.AbbreviatorConverter;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.DurationConverter;
import org.jumpmind.vaadin.ui.common.ResizableWindow;

import com.vaadin.v7.data.util.BeanContainer;
import com.vaadin.v7.data.util.filter.SimpleStringFilter;
import com.vaadin.v7.event.FieldEvents.TextChangeEvent;
import com.vaadin.v7.event.FieldEvents.TextChangeListener;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.v7.ui.Grid;
import com.vaadin.v7.ui.Grid.HeaderCell;
import com.vaadin.v7.ui.Grid.HeaderRow;
import com.vaadin.v7.ui.Grid.RowDescriptionGenerator;
import com.vaadin.v7.ui.Grid.RowReference;
import com.vaadin.v7.ui.Grid.SelectionMode;
import com.vaadin.v7.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.v7.ui.renderers.DateRenderer;
import com.vaadin.ui.themes.ValoTheme;

public class SqlHistoryDialog extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    private final Grid table;

    private QueryPanel queryPanel;

    private ISettingsProvider settingsProvider;

    public SqlHistoryDialog(ISettingsProvider settingsProvider, QueryPanel queryPanel) {
        super("Sql History");
        this.settingsProvider = settingsProvider;
        this.queryPanel = queryPanel;

        VerticalLayout mainLayout = new VerticalLayout();
        mainLayout.setSizeFull();
        mainLayout.setMargin(false);
        mainLayout.setSpacing(true);
        addComponent(mainLayout, 1);

        final Set<SqlHistory> sqlHistories = new TreeSet<SqlHistory>(settingsProvider.get().getSqlHistory());

        table = new Grid();

        table.addColumn("sqlStatement", String.class).setHeaderCaption("SQL").setConverter(new AbbreviatorConverter(50));

        table.addColumn("lastExecuteTime", Date.class).setHeaderCaption("Time").setWidth(150).setMaximumWidth(200)
                .setRenderer(new DateRenderer("%1$tY-%1$tm-%1$td %1$tk:%1$tM:%1$tS"));

        table.addColumn("lastExecuteDuration", Long.class).setHeaderCaption("Duration").setWidth(120).setConverter(new DurationConverter());

        table.addColumn("executeCount", Long.class).setHeaderCaption("Count").setWidth(120);
        table.setEditorEnabled(false);
        table.setSelectionMode(SelectionMode.MULTI);
        table.setRowDescriptionGenerator(new RowDescriptionGenerator() {

            private static final long serialVersionUID = 1L;

            @Override
            public String getDescription(RowReference row) {
                return (String) row.getItemId();
            }
        });
        final BeanContainer<String, SqlHistory> container = new BeanContainer<String, SqlHistory>(SqlHistory.class);
        container.setBeanIdProperty("sqlStatement");

        HeaderRow filteringHeader = table.appendHeaderRow();
        HeaderCell logTextFilterCell = filteringHeader.getCell("sqlStatement");
        TextField filterField = new TextField();
        filterField.setInputPrompt("Filter");
        filterField.addStyleName(ValoTheme.TEXTFIELD_TINY);
        filterField.setWidth("100%");

        // Update filter When the filter input is changed
        filterField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                // Can't modify filters so need to replace
                container.removeContainerFilters("sqlStatement");

                // (Re)create the filter if necessary
                if (!event.getText().isEmpty()) {
                    container.addContainerFilter(new SimpleStringFilter("sqlStatement", event.getText(), true, false));
                }

            }
        });
        logTextFilterCell.setComponent(filterField);

        table.setContainerDataSource(container);

        table.addItemClickListener(new ItemClickEvent.ItemClickListener() {

            private static final long serialVersionUID = 1L;

            public void itemClick(ItemClickEvent event) {
                Object object = event.getPropertyId();
                if (object != null && !object.toString().equals("")) {
                    if (event.isDoubleClick()) {
                        table.select(event.getItemId());
                        select();
                    } else {
                        Object row = event.getItemId();
                        if (!table.getSelectedRows().contains(row)) {
                            table.select(row);
                        } else {
                            table.deselect(row);
                        }
                    }
                }
            }
        });

        table.setSizeFull();

        mainLayout.addComponent(table);
        mainLayout.setExpandRatio(table, 1);

        container.addAll(sqlHistories);

        Button cancelButton = new Button("Cancel");
        cancelButton.addClickListener(new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                close();
            }
        });

        Button applyButton = CommonUiUtils.createPrimaryButton("Select");
        applyButton.setClickShortcut(KeyCode.ENTER);
        applyButton.addClickListener(new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                select();
            }
        });

        addComponent(buildButtonFooter(cancelButton, applyButton));

    }

    protected void select() {
        List<Object> values = new ArrayList<Object>(table.getSelectedRows());
        Collections.reverse(values);
        if (values != null && values.size() > 0) {
            String delimiter = settingsProvider.get().getProperties().get(Settings.SQL_EXPLORER_DELIMITER);
            for (Object sql : values) {
                queryPanel.appendSql(sql + (sql.toString().trim().endsWith(delimiter) ? "" : delimiter));
            }
            close();
        }
    }
}
