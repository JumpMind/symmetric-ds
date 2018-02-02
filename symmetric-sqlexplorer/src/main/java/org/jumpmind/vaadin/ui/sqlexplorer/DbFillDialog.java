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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.DbFill;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ConfirmDialog;
import org.jumpmind.vaadin.ui.common.ResizableWindow;
import org.jumpmind.vaadin.ui.common.ConfirmDialog.IConfirmListener;

import com.vaadin.v7.data.Item;
import com.vaadin.v7.event.FieldEvents.TextChangeEvent;
import com.vaadin.v7.event.FieldEvents.TextChangeListener;
import com.vaadin.v7.ui.AbstractTextField.TextChangeEventMode;
import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.v7.ui.CheckBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.v7.ui.OptionGroup;
import com.vaadin.v7.ui.TextField;
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

    private OptionGroup oGroup;

    private QueryPanel queryPanel;

    private IDatabasePlatform databasePlatform;

    public DbFillDialog(IDatabasePlatform databasePlatform, QueryPanel queryPanel) {
        this(databasePlatform, new HashSet<Table>(0), queryPanel);
    }

    public DbFillDialog(IDatabasePlatform databasePlatform, Set<Table> selectedTableSet,
            QueryPanel queryPanel) {
        super("Database Fill");
        setModal(true);
        setHeight(500, Unit.PIXELS);
        setWidth(605, Unit.PIXELS);

        this.databasePlatform = databasePlatform;
        this.queryPanel = queryPanel;

        tableSelectionLayout = new TableSelectionLayout(databasePlatform, selectedTableSet) {
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
        countField.setTextChangeEventMode(TextChangeEventMode.EAGER);
        countField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                countField.setValue(event.getText());
                fillButton.setEnabled(enableFillButton());
            }
        });
        formLayout.addComponent(countField);

        intervalField = new TextField("Interval (ms)");
        intervalField.setValue("0");
        intervalField.setTextChangeEventMode(TextChangeEventMode.EAGER);
        intervalField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                intervalField.setValue(event.getText());
                fillButton.setEnabled(enableFillButton());
            }
        });
        formLayout.addComponent(intervalField);

        insertWeightField = new TextField("Insert Weight");
        insertWeightField.setValue("1");
        insertWeightField.setTextChangeEventMode(TextChangeEventMode.EAGER);
        insertWeightField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                insertWeightField.setValue(event.getText());
                fillButton.setEnabled(enableFillButton());
            }
        });
        formLayout.addComponent(insertWeightField);

        updateWeightField = new TextField("Update Weight");
        updateWeightField.setValue("0");
        updateWeightField.setTextChangeEventMode(TextChangeEventMode.EAGER);
        updateWeightField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                updateWeightField.setValue(event.getText());
                fillButton.setEnabled(enableFillButton());
            }
        });
        formLayout.addComponent(updateWeightField);

        deleteWeightField = new TextField("Delete Weight");
        deleteWeightField.setValue("0");
        deleteWeightField.setTextChangeEventMode(TextChangeEventMode.EAGER);
        deleteWeightField.addTextChangeListener(new TextChangeListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void textChange(TextChangeEvent event) {
                deleteWeightField.setValue(event.getText());
                fillButton.setEnabled(enableFillButton());
            }
        });
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
        
        oGroup = new OptionGroup();
        oGroup.addItem("Fill Table(s)");
        oGroup.addItem("Send to Sql Editor");
        oGroup.select("Fill Table(s)");
        oGroup.setNullSelectionAllowed(false);
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
        //dbFill.setCascadingSelect(cascadeSelectBox.getValue());
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

    @SuppressWarnings("unchecked")
    public List<String> getSelectedTables() {
        tableSelectionLayout.listOfTablesTable.commit();
        List<String> select = new ArrayList<String>();
        Collection<Object> itemIds = (Collection<Object>) tableSelectionLayout.listOfTablesTable
                .getItemIds();
        for (Object itemId : itemIds) {
            Item item = tableSelectionLayout.listOfTablesTable.getItem(itemId);
            CheckBox checkBox = (CheckBox) item.getItemProperty("selected").getValue();
            if (checkBox.getValue().equals(Boolean.TRUE) && checkBox.isEnabled()) {
                select.add((String) itemId);
            }
        }
        return select;
    }
}
