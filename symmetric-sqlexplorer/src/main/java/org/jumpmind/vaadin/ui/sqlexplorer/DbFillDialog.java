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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.DbFill;
import org.jumpmind.symmetric.io.data.DmlWeight;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ConfirmDialog;
import org.jumpmind.vaadin.ui.common.ConfirmDialog.IConfirmListener;
import org.jumpmind.vaadin.ui.common.ResizableWindow;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.shared.ui.ValueChangeMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.RadioButtonGroup;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

public class DbFillDialog extends ResizableWindow {
    private static final long serialVersionUID = 1L;
    private Button cancelButton;
    private Button nextButton;
    private Button previousButton;
    private Button fillButton;
    private TableSelectionLayout tableSelectionLayout;
    private VerticalLayout optionLayout;
    private CheckBox continueBox;
    private CheckBox cascadeBox;
    private CheckBox cascadeSelectBox;
    private CheckBox truncateBox;
    private CheckBox verboseBox;
    private TextField countField;
    private TextField intervalField;
    private TextField insertWeightField;
    private TextField updateWeightField;
    private TextField deleteWeightField;
    private DbFill dbFill;
    private RadioButtonGroup<String> oGroup;
    private QueryPanel queryPanel;
    private IDatabasePlatform databasePlatform;

    public DbFillDialog(IDatabasePlatform databasePlatform, QueryPanel queryPanel, String excludeTablesRegex) {
        this(databasePlatform, new HashSet<Table>(0), queryPanel, excludeTablesRegex);
    }

    public DbFillDialog(IDatabasePlatform databasePlatform, Set<Table> selectedTableSet,
            QueryPanel queryPanel, String excludeTablesRegex) {
        super("Database Fill");
        setModal(true);
        setHeight(500, Unit.PIXELS);
        setWidth(605, Unit.PIXELS);
        this.databasePlatform = databasePlatform;
        this.queryPanel = queryPanel;
        tableSelectionLayout = new TableSelectionLayout(databasePlatform, selectedTableSet, excludeTablesRegex) {
            private static final long serialVersionUID = 1L;

            protected void selectionChanged() {
                nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
            };
        };
        createOptionLayout();
        addComponent(tableSelectionLayout, 1);
        addButtons();
        nextButton.setClickShortcut(KeyCode.ENTER);
        nextButton.focus();
    }

    protected void addButtons() {
        nextButton = CommonUiUtils.createPrimaryButton("Next", new ClickListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event) {
                next();
            }
        });
        nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
        cancelButton = new Button("Close", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                close();
            }
        });
        previousButton = new Button("Previous", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                previous();
            }
        });
        previousButton.setVisible(false);
        fillButton = CommonUiUtils.createPrimaryButton("Fill...", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                createDbFill();
                if (dbFill.getPrint() == false) {
                    confirm();
                } else {
                    List<String> tables = getSelectedTables();
                    for (String tableName : tables) {
                        Table table = databasePlatform.getTableFromCache(
                                tableSelectionLayout.catalogSelect.getValue() != null ? tableSelectionLayout.catalogSelect
                                        .getValue().toString() : null,
                                tableSelectionLayout.schemaSelect.getValue() != null ? tableSelectionLayout.schemaSelect
                                        .getValue().toString() : null, tableName, false);
                        if (table != null) {
                            for (int i = 0; i < dbFill.getRecordCount(); i++) {
                                for (int j = 0; j < dbFill.getInsertWeight(); j++) {
                                    String sql = dbFill.createDynamicRandomInsertSql(table);
                                    queryPanel.appendSql(sql);
                                }
                                for (int j = 0; j < dbFill.getUpdateWeight(); j++) {
                                    String sql = dbFill.createDynamicRandomUpdateSql(table);
                                    queryPanel.appendSql(sql);
                                }
                                for (int j = 0; j < dbFill.getDeleteWeight(); j++) {
                                    String sql = dbFill.createDynamicRandomDeleteSql(table);
                                    queryPanel.appendSql(sql);
                                }
                            }
                        }
                    }
                    UI.getCurrent().removeWindow(DbFillDialog.this);
                }
            }
        });
        fillButton.setVisible(false);
        addComponent(buildButtonFooter(cancelButton, previousButton, nextButton, fillButton));
    }

    protected void createOptionLayout() {
        optionLayout = new VerticalLayout();
        optionLayout.addStyleName("v-scrollable");
        optionLayout.setMargin(true);
        optionLayout.setSpacing(true);
        optionLayout.setSizeFull();
        optionLayout.addComponent(new Label("Please choose from the following options"));
        FormLayout formLayout = new FormLayout();
        formLayout.setSizeFull();
        optionLayout.addComponent(formLayout);
        optionLayout.setExpandRatio(formLayout, 1);
        countField = new TextField("Count (# of rows to fill)");
        countField.setValue("1");
        countField.setValueChangeMode(ValueChangeMode.EAGER);
        countField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.addComponent(countField);
        intervalField = new TextField("Interval (ms)");
        intervalField.setValue("0");
        intervalField.setValueChangeMode(ValueChangeMode.EAGER);
        intervalField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.addComponent(intervalField);
        insertWeightField = new TextField("Insert Weight");
        insertWeightField.setValue("1");
        insertWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        insertWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.addComponent(insertWeightField);
        updateWeightField = new TextField("Update Weight");
        updateWeightField.setValue("0");
        updateWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        updateWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.addComponent(updateWeightField);
        deleteWeightField = new TextField("Delete Weight");
        deleteWeightField.setValue("0");
        deleteWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        deleteWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.addComponent(deleteWeightField);
        continueBox = new CheckBox("Continue (ignore ANY errors and continue to modify)");
        continueBox.setValue(true);
        formLayout.addComponent(continueBox);
        cascadeBox = new CheckBox("Fill dependent tables also.");
        cascadeBox.setValue(false);
        formLayout.addComponent(cascadeBox);
        cascadeSelectBox = new CheckBox("Fill dependent tables by selecting existing data.");
        cascadeSelectBox.setValue(false);
        formLayout.addComponent(cascadeSelectBox);
        verboseBox = new CheckBox("Turn on verbose logging during fill.");
        verboseBox.setValue(false);
        formLayout.addComponent(verboseBox);
        truncateBox = new CheckBox("Truncate table(s) before filling.");
        truncateBox.setValue(false);
        formLayout.addComponent(truncateBox);
        oGroup = new RadioButtonGroup<String>();
        oGroup.setItems("Fill Table(s)", "Send to Sql Editor");
        oGroup.setSelectedItem("Fill Table(s)");
        formLayout.addComponent(oGroup);
    }

    protected void confirm() {
        ConfirmDialog
                .show("Confirm",
                        "Are you sure?  Please note that this will effect data in the selected tables.  Make sure you have a backup of your data.",
                        new IConfirmListener() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean onOk() {
                                fill();
                                close();
                                return true;
                            }
                        });
    }

    protected void fill() {
        List<String> temp = getSelectedTables();
        String[] tables = new String[temp.size()];
        temp.toArray(tables);
        dbFill.fillTables(tables);
    }

    protected void createDbFill() {
        dbFill = new DbFill(databasePlatform);
        dbFill.setCatalog(tableSelectionLayout.catalogSelect.getValue() != null ? tableSelectionLayout.catalogSelect
                .getValue().toString() : null);
        dbFill.setSchema(tableSelectionLayout.schemaSelect.getValue() != null ? tableSelectionLayout.schemaSelect
                .getValue().toString() : null);
        dbFill.setContinueOnError(continueBox.getValue());
        dbFill.setCascading(cascadeBox.getValue());
        // TODO these can be made available when SymmetricDS dependency is pointed to 3.8
        dbFill.setCascadingSelect(cascadeSelectBox.getValue());
        dbFill.setVerbose(verboseBox.getValue());
        // dbFill.setTruncate(truncateBox.getValue());
        dbFill.setRecordCount(Integer.parseInt(countField.getValue().toString()));
        dbFill.setInterval(Integer.parseInt(intervalField.getValue().toString()));
        dbFill.setDmlWeight(new DmlWeight(Integer.parseInt(insertWeightField.getValue()), Integer.parseInt(updateWeightField.getValue()),
                Integer.parseInt(deleteWeightField.getValue())));
        if (oGroup.getValue().toString().equals("Send to Sql Editor")) {
            dbFill.setPrint(true);
        }
    }

    protected void previous() {
        content.removeComponent(optionLayout);
        content.addComponent(tableSelectionLayout, 0);
        content.setExpandRatio(tableSelectionLayout, 1);
        fillButton.setVisible(false);
        fillButton.removeClickShortcut();
        previousButton.setVisible(false);
        nextButton.setVisible(true);
        nextButton.setClickShortcut(KeyCode.ENTER);
        nextButton.focus();
    }

    protected void next() {
        content.removeComponent(tableSelectionLayout);
        content.addComponent(optionLayout, 0);
        content.setExpandRatio(optionLayout, 1);
        nextButton.setVisible(false);
        nextButton.removeClickShortcut();
        previousButton.setVisible(true);
        fillButton.setVisible(true);
        fillButton.setClickShortcut(KeyCode.ENTER);
        fillButton.focus();
    }

    protected boolean enableFillButton() {
        if (!countField.getValue().equals("")) {
            if (!intervalField.getValue().equals("")) {
                if (!insertWeightField.getValue().equals("")) {
                    if (!updateWeightField.getValue().equals("")) {
                        if (!deleteWeightField.getValue().equals("")) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public List<String> getSelectedTables() {
        return new ArrayList<String>(tableSelectionLayout.listOfTablesGrid.getSelectedItems());
    }
}
