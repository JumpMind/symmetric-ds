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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ResizableWindow;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Grid.SelectionMode;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.components.grid.HeaderCell;
import com.vaadin.ui.components.grid.HeaderRow;
import com.vaadin.ui.themes.ValoTheme;

public class SqlHistoryDialog extends ResizableWindow {
    private static final long serialVersionUID = 1L;
    private final Grid<SqlHistory> grid;
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
        grid = new Grid<SqlHistory>();
        grid.setSelectionMode(SelectionMode.MULTI);
        grid.addColumn(history -> StringUtils.abbreviate(history.getSqlStatement(), 50)).setId("sqlStatement").setCaption("SQL");
        grid.addColumn(history -> String.format("%1$tY-%1$tm-%1$td %1$tk:%1$tM:%1$tS", history.getLastExecuteTime()))
                .setCaption("Time").setWidth(150).setMaximumWidth(200);
        grid.addColumn(history -> CommonUiUtils.formatDuration(history.getLastExecuteDuration())).setCaption("Duration").setWidth(120);
        grid.addColumn(history -> history.getExecuteCount()).setCaption("Count").setWidth(120);
        grid.setDescriptionGenerator(history -> history.getSqlStatement());
        HeaderRow filteringHeader = grid.appendHeaderRow();
        HeaderCell logTextFilterCell = filteringHeader.getCell("sqlStatement");
        TextField filterField = new TextField();
        filterField.setPlaceholder("Filter");
        filterField.addStyleName(ValoTheme.TEXTFIELD_TINY);
        filterField.setWidth("100%");
        // Update filter When the filter input is changed
        filterField.addValueChangeListener(event -> filter(event.getValue()));
        logTextFilterCell.setComponent(filterField);
        grid.addItemClickListener(event -> {
            if (event.getColumn() != null) {
                grid.deselectAll();
                grid.select(event.getItem());
            }
            if (event.getMouseEventDetails().isDoubleClick()) {
                select();
            }
        });
        grid.setSizeFull();
        mainLayout.addComponent(grid);
        mainLayout.setExpandRatio(grid, 1);
        grid.setItems(sqlHistories);
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

    private void filter(String filter) {
        final Set<SqlHistory> histories = new TreeSet<SqlHistory>(settingsProvider.get().getSqlHistory());
        List<SqlHistory> filteredHistories = new ArrayList<SqlHistory>();
        for (SqlHistory history : histories) {
            if (StringUtils.isBlank(filter) || history.getSqlStatement().toLowerCase().contains(filter.toLowerCase())) {
                filteredHistories.add(history);
            }
        }
        grid.setItems(filteredHistories);
    }

    protected void select() {
        List<SqlHistory> histories = new ArrayList<SqlHistory>(grid.getSelectedItems());
        Collections.reverse(histories);
        if (histories != null && histories.size() > 0) {
            String delimiter = settingsProvider.get().getProperties().get(Settings.SQL_EXPLORER_DELIMITER);
            for (SqlHistory history : histories) {
                String sql = history.getSqlStatement();
                queryPanel.appendSql(sql + (sql.trim().endsWith(delimiter) ? "" : delimiter));
            }
            close();
        }
    }
}
