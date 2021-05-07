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
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ConfirmDialog;
import org.jumpmind.vaadin.ui.common.ResizableDialog;
import org.jumpmind.vaadin.ui.common.ConfirmDialog.IConfirmListener;

import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.ShortcutRegistration;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.radiobutton.RadioButtonGroup;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.value.ValueChangeMode;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class DbFillDialog extends ResizableDialog {

    private static final long serialVersionUID = 1L;

    private Button cancelButton;

    private Button nextButton;
    
    private ShortcutRegistration nextShortcutRegistration;

    private Button previousButton;

    private Button fillButton;
    
    private ShortcutRegistration fillShortcutRegistration;

    private TableSelectionLayout tableSelectionLayout;

    private VerticalLayout optionLayout;

    private Checkbox continueBox;

    private Checkbox cascadeBox;

    private Checkbox cascadeSelectBox;

    private Checkbox truncateBox;

    private Checkbox verboseBox;
 
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
        setHeight("500px");
        setWidth("605px");

        this.databasePlatform = databasePlatform;
        this.queryPanel = queryPanel;

        tableSelectionLayout = new TableSelectionLayout(databasePlatform, selectedTableSet, excludeTablesRegex) {
            private static final long serialVersionUID = 1L;

            protected void selectionChanged() {
                nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
            };
        };

        createOptionLayout();

        add(tableSelectionLayout, 1);
        addButtons();
        nextShortcutRegistration = nextButton.addClickShortcut(Key.ENTER);
        nextButton.focus();
    }

    protected void addButtons() {
        nextButton = CommonUiUtils.createPrimaryButton("Next", event -> next());
        nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);

        cancelButton = new Button("Close", event -> close());

        previousButton = new Button("Previous", event -> previous());
        previousButton.setVisible(false);

        fillButton = CommonUiUtils.createPrimaryButton("Fill...", event -> {
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
                close();
            }
        });
        fillButton.setVisible(false);

        add(buildButtonFooter(cancelButton, previousButton, nextButton, fillButton));

    }

    protected void createOptionLayout() {
        optionLayout = new VerticalLayout();
        optionLayout.addClassName("v-scrollable");
        optionLayout.setMargin(true);
        optionLayout.setSpacing(true);
        optionLayout.setSizeFull();
        optionLayout.add(new Span("Please choose from the following options"));

        FormLayout formLayout = new FormLayout();
        formLayout.setSizeFull();
        optionLayout.add(formLayout);
        optionLayout.expand(formLayout);

        countField = new TextField("Count (# of rows to fill)");
        countField.setValue("1");
        countField.setValueChangeMode(ValueChangeMode.EAGER);
        countField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.add(countField);

        intervalField = new TextField("Interval (ms)");
        intervalField.setValue("0");
        intervalField.setValueChangeMode(ValueChangeMode.EAGER);
        intervalField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.add(intervalField);

        insertWeightField = new TextField("Insert Weight");
        insertWeightField.setValue("1");
        insertWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        insertWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.add(insertWeightField);

        updateWeightField = new TextField("Update Weight");
        updateWeightField.setValue("0");
        updateWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        updateWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.add(updateWeightField);

        deleteWeightField = new TextField("Delete Weight");
        deleteWeightField.setValue("0");
        deleteWeightField.setValueChangeMode(ValueChangeMode.EAGER);
        deleteWeightField.addValueChangeListener(event -> fillButton.setEnabled(enableFillButton()));
        formLayout.add(deleteWeightField);

        continueBox = new Checkbox("Continue (ignore ANY errors and continue to modify)");
        continueBox.setValue(true);
        formLayout.add(continueBox);

        cascadeBox = new Checkbox("Fill dependent tables also.");
        cascadeBox.setValue(false);
        formLayout.add(cascadeBox);

        cascadeSelectBox = new Checkbox("Fill dependent tables by selecting existing data.");
        cascadeSelectBox.setValue(false);
        formLayout.add(cascadeSelectBox);

        verboseBox = new Checkbox("Turn on verbose logging during fill.");
        verboseBox.setValue(false);
        formLayout.add(verboseBox);

        truncateBox = new Checkbox("Truncate table(s) before filling.");
        truncateBox.setValue(false);
        formLayout.add(truncateBox);
        
        oGroup = new RadioButtonGroup<String>();
        oGroup.setItems("Fill Table(s)", "Send to Sql Editor");
        oGroup.setValue("Fill Table(s)");
        formLayout.add(oGroup);

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
        //dbFill.setTruncate(truncateBox.getValue());
        dbFill.setRecordCount(Integer.parseInt(countField.getValue().toString()));
        dbFill.setInterval(Integer.parseInt(intervalField.getValue().toString()));
        int[] weights = new int[3];
        weights[0] = Integer.parseInt(insertWeightField.getValue().toString());
        weights[1] = Integer.parseInt(updateWeightField.getValue().toString());
        weights[2] = Integer.parseInt(deleteWeightField.getValue().toString());
        dbFill.setDmlWeight(weights);
        if (oGroup.getValue().toString().equals("Send to Sql Editor")) {
            dbFill.setPrint(true);
        }
    }

    protected void previous() {
        content.remove(optionLayout);
        content.addComponentAtIndex(0, tableSelectionLayout);
        content.expand(tableSelectionLayout);
        fillButton.setVisible(false);
        if (fillShortcutRegistration != null) {
            fillShortcutRegistration.remove();
        }
        previousButton.setVisible(false);
        nextButton.setVisible(true);
        nextButton.addClickShortcut(Key.ENTER);
        nextButton.focus();
    }

    protected void next() {
        content.remove(tableSelectionLayout);
        content.addComponentAtIndex(0, optionLayout);
        content.expand(optionLayout);
        nextButton.setVisible(false);
        nextShortcutRegistration.remove();
        previousButton.setVisible(true);
        fillButton.setVisible(true);
        fillShortcutRegistration = fillButton.addClickShortcut(Key.ENTER);
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
