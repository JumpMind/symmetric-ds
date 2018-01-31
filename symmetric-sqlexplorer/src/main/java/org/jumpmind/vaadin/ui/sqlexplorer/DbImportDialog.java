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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.DbImport;
import org.jumpmind.symmetric.io.data.DbImport.Format;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ResizableWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.ui.AbstractLayout;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.Upload;
import com.vaadin.ui.Upload.Receiver;
import com.vaadin.ui.Upload.SucceededEvent;
import com.vaadin.ui.Upload.SucceededListener;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;

public class DbImportDialog extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    final Logger log = LoggerFactory.getLogger(getClass());
    
    enum DbImportFormat {
        SQL, XML, CSV, SYM_XML;
    }

    private Set<Table> selectedTablesSet;

    private Table selectedTable;

    private VerticalLayout importLayout;

    private ComboBox<DbImportFormat> formatSelect;

    private CheckBox force;

    private CheckBox ignore;

    private CheckBox replace;

    private ComboBox<String> schemaSelect;

    private ComboBox<String> catalogSelect;

    private ComboBox<String> listOfTablesSelect;

    private TextField commitField;

    private CheckBox alter;

    private CheckBox alterCase;

    private Button cancelButton;

    private DbImport dbImport;

    private Upload upload;

    private Format format;

    private IDatabasePlatform databasePlatform;

    private File file;

    private FileOutputStream out;

    public DbImportDialog(IDatabasePlatform databasePlatform) {
        this(databasePlatform, new HashSet<Table>(0));
    }
    
    public DbImportDialog(IDatabasePlatform databasePlatform, Set<Table> selectedTableSet) {
        super("Database Import");

        this.selectedTablesSet = selectedTableSet;
        this.databasePlatform = databasePlatform;

        createImportLayout();
    }

    protected void createImportLayout() {
        importLayout = new VerticalLayout();
        importLayout.setSizeFull();
        importLayout.addStyleName("v-scrollable");
        importLayout.setMargin(true);
        importLayout.setSpacing(true);

        importLayout.addComponent(new Label("Please select from the following options"));

        FormLayout formLayout = new FormLayout();
        formLayout.setSizeFull();
        importLayout.addComponent(formLayout);
        importLayout.setExpandRatio(formLayout, 1);

        formatSelect = new ComboBox<>("Format");
        formatSelect.setItems( DbImportFormat.values());
        formatSelect.setEmptySelectionAllowed(false);
        formatSelect.setValue(DbImportFormat.SQL);
        formatSelect.addValueChangeListener((e) -> {
                DbImportFormat format = (DbImportFormat) formatSelect.getValue();

                switch (format) {
                    case SQL:
                        listOfTablesSelect.setEnabled(false);
                        alter.setEnabled(false);
                        alterCase.setEnabled(false);
                        break;
                    case XML:
                        listOfTablesSelect.setEnabled(false);
                        alter.setEnabled(true);
                        alterCase.setEnabled(true);
                        break;
                    case CSV:
                        listOfTablesSelect.setEnabled(true);
                        alter.setEnabled(false);
                        alterCase.setEnabled(false);
                        break;
                    case SYM_XML:
                        listOfTablesSelect.setEnabled(false);
                        alter.setEnabled(false);
                        alterCase.setEnabled(false);
                        break;
                }
        });
        formLayout.addComponent(formatSelect);

        catalogSelect = new ComboBox<>("Catalog");
        catalogSelect.setItems(getCatalogs());
        catalogSelect.setValue(databasePlatform.getDefaultCatalog());
        formLayout.addComponent(catalogSelect);

        schemaSelect = new ComboBox<>("Schema");
        schemaSelect.setItems(getSchemas());
        
        if (selectedTablesSet.size() > 0) {
            schemaSelect.setValue(selectedTablesSet.iterator().next().getSchema());
        } else {
            schemaSelect.setValue(databasePlatform.getDefaultSchema());
        }
        schemaSelect.addValueChangeListener((e) -> {
                populateListOfTablesSelect();
            
        });
        formLayout.addComponent(schemaSelect);

        listOfTablesSelect = new ComboBox<>("Tables");
        populateListOfTablesSelect();
        listOfTablesSelect.setEnabled(false);

        if (!this.selectedTablesSet.isEmpty()) {
            if (this.selectedTablesSet.size() == 1) {
                this.selectedTable = this.selectedTablesSet.iterator().next();
                listOfTablesSelect.setValue(this.selectedTable.getName());
                this.selectedTablesSet.clear();
            } else {
                List<Table> list = new ArrayList<Table>(this.selectedTablesSet);
                listOfTablesSelect.setValue(list.get(0).getName());
                this.selectedTable = list.get(0);
                this.selectedTablesSet.clear();
            }
        }
        formLayout.addComponent(listOfTablesSelect);

        commitField = new TextField("Rows to Commit");
        commitField.addValueChangeListener((event) -> {
                commitField.setValue(event.getValue());
        });
        commitField.setValueChangeTimeout(200);
        commitField.setValue("10000");
        formLayout.addComponent(commitField);

        force = new CheckBox("Force");
        formLayout.addComponent(force);

        ignore = new CheckBox("Ignore");
        formLayout.addComponent(ignore);

        replace = new CheckBox("Replace");
        formLayout.addComponent(replace);

        alter = new CheckBox("Alter");
        alter.setEnabled(false);
        formLayout.addComponent(alter);

        alterCase = new CheckBox("Alter Case");
        alterCase.setEnabled(false);
        formLayout.addComponent(alterCase);

        upload = new Upload(null, new Receiver() {

            private static final long serialVersionUID = 1L;

            @Override
            public OutputStream receiveUpload(String filename, String mimeType) {
                try {
                    file = File.createTempFile("dbimport", formatSelect.getValue().toString());
                    out = new FileOutputStream(file);
                    return new BufferedOutputStream(new FileOutputStream(file));
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                    CommonUiUtils.notify("Failed to import " + filename, e);
                }
                return null;
            }
        });
        upload.addSucceededListener(new SucceededListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void uploadSucceeded(SucceededEvent event) {
                createDbImport();
                try {
                    doDbImport();
                    close();
                    Notification.show("Successful Import", Type.HUMANIZED_MESSAGE);
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                    Notification.show(e.getMessage(), Type.ERROR_MESSAGE);
                } finally {
                    deleteFileAndResource();
                }
            }
        });        
        upload.setButtonCaption("Import");
        upload.setStyleName(ValoTheme.BUTTON_PRIMARY);
        formLayout.addComponent(upload);

        cancelButton = new Button("Cancel", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                UI.getCurrent().removeWindow(DbImportDialog.this);
            }
        });
        addComponent(importLayout, 1);
        AbstractLayout buttonLayout = buildButtonFooter(cancelButton);
        buttonLayout.addComponent(upload);
        addComponent(buttonLayout);
    }

    protected void deleteFileAndResource() {
        try {
            out.close();
            file.delete();
        } catch (IOException e) {
            log.warn(e.getMessage(), e);
            Notification.show(e.getMessage());
        }
    }

    protected void doDbImport() throws FileNotFoundException {
        if (format.toString().equals("CSV")) {
            dbImport.importTables(new BufferedInputStream(new FileInputStream(file)),
                    listOfTablesSelect.getValue().toString());
        } else {
            dbImport.importTables(new BufferedInputStream(new FileInputStream(file)));
        }
    }

    protected void createDbImport() {
        dbImport = new DbImport(databasePlatform);

        format = DbImport.Format.valueOf(formatSelect.getValue().toString());
        dbImport.setFormat(format);
        dbImport.setCatalog((String)catalogSelect.getValue());
        dbImport.setSchema((String)schemaSelect.getValue());
        dbImport.setCommitRate(Long.parseLong(commitField.getValue()));
        dbImport.setForceImport(force.getValue());
        dbImport.setIgnoreCollisions(ignore.getValue());
        dbImport.setIgnoreMissingTables(ignore.getValue());
        dbImport.setAlterTables(alter.getValue());
        dbImport.setAlterCaseToMatchDatabaseDefaultCase(alterCase.getValue());

    }

    protected boolean importButtonEnable() {
        if (formatSelect.getValue() != null) {
                    if (!commitField.getValue().equals("")) {
                        if (formatSelect.getValue().toString().equals("CSV")) {
                            if (listOfTablesSelect.getValue() != null) {
                                return true;
                            }
                        } else {
                            return true;
                        }
            }
        }
        return false;
    }

    protected void populateListOfTablesSelect() {
        listOfTablesSelect.clear();
        listOfTablesSelect.setItems(getTables());
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

}
