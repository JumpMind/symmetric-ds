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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Compatible;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ResizableWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.server.FileDownloader;
import com.vaadin.server.StreamResource;
import com.vaadin.server.StreamResource.StreamSource;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.RadioButtonGroup;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;

public class DbExportDialog extends ResizableWindow {

    private static final String EXPORT_TO_THE_SQL_EDITOR = "Export to the SQL Editor";

    private static final String EXPORT_AS_A_FILE = "Export as a File";

    private static final long serialVersionUID = 1L;

    final Logger log = LoggerFactory.getLogger(getClass());

    enum DbExportFormat {
        SQL, XML, CSV, SYM_XML, CSV_DQUOTE;
    }
    
    private ComboBox<DbExportFormat> formatSelect;

    private ComboBox<Compatible> compatibilitySelect;

    private CheckBox data;

    private CheckBox createInfo;

    private CheckBox foreignKeys;

    private CheckBox indices;

    private CheckBox quotedIdentifiers;

    private CheckBox dropTables;

    private TextArea whereClauseField;

    private Button previousButton;

    private Button cancelButton;

    private Button selectAllLink;

    private Button selectNoneLink;

    public Button nextButton;

    private Button exportFileButton;

    private Button exportEditorButton;
    
    private Button doneButton;

    private TableSelectionLayout tableSelectionLayout;

    private VerticalLayout optionLayout;

    private FileDownloader fileDownloader;

    private DbExport dbExport;

    private RadioButtonGroup<String> exportFormatOptionGroup;

    private QueryPanel queryPanel;

    private IDatabasePlatform databasePlatform;

    public DbExportDialog(IDatabasePlatform databasePlatform, QueryPanel queryPanel, String excludeTablesRegex) {
        this(databasePlatform, new HashSet<Table>(), queryPanel, excludeTablesRegex);
    }

    public DbExportDialog(IDatabasePlatform databasePlatform, Set<Table> selectedTableSet,
            QueryPanel queryPanel, String excludeTablesRegex) {
        super("Database Export");

        this.databasePlatform = databasePlatform;
        this.queryPanel = queryPanel;

        tableSelectionLayout = new TableSelectionLayout(databasePlatform, selectedTableSet, excludeTablesRegex) {
            private static final long serialVersionUID = 1L;

            @Override
            protected void selectionChanged() {
                nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
            }
        };

        createOptionLayout();

        addComponent(tableSelectionLayout, 1);

        addButtons();
        
        nextButton.setClickShortcut(KeyCode.ENTER);
        nextButton.focus();
    }

    protected void addButtons() {
        selectAllLink = new Button("Select All");
        selectAllLink.addStyleName(ValoTheme.BUTTON_LINK);
        selectAllLink.addClickListener(new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                tableSelectionLayout.selectAll();
            }
        });

        selectNoneLink = new Button("Select None");
        selectNoneLink.addStyleName(ValoTheme.BUTTON_LINK);
        selectNoneLink.addClickListener(new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                tableSelectionLayout.selectNone();
            }
        });

        nextButton = CommonUiUtils.createPrimaryButton("Next");
        nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
        nextButton.addClickListener(new Button.ClickListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event) {
                next();
            }
        });

        cancelButton = new Button("Cancel", new Button.ClickListener() {
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

        exportFileButton = CommonUiUtils.createPrimaryButton("Export", new Button.ClickListener(){
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                exportFileButton.removeClickShortcut();
                fileDownloader.setFileDownloadResource(createResource());
                doneButton.setClickShortcut(KeyCode.ENTER);
                doneButton.focus();
            }
        });
        buildFileDownloader();
        exportFileButton.setVisible(false);

        exportEditorButton = CommonUiUtils.createPrimaryButton("Export", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                exportToEditor();
                close();
            }
        });
        exportEditorButton.setVisible(false);
        
        doneButton = new Button("Close", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                close();
            }
        });
        doneButton.setVisible(false);

        addComponent(buildButtonFooter(new Button[] { selectAllLink, selectNoneLink },
                cancelButton, previousButton, nextButton, exportFileButton, exportEditorButton, doneButton));

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

        formatSelect = new ComboBox<DbExportFormat>("Format", Arrays.asList(DbExportFormat.values()));
        formatSelect.setEmptySelectionAllowed(false);
        formatSelect.setValue(DbExportFormat.SQL);
        formatSelect.addValueChangeListener(event -> {
            switch (formatSelect.getValue()) {
                case SQL:
                    compatibilitySelect.setEnabled(true);
                    compatibilitySelect.setEmptySelectionAllowed(false);
                    setDefaultCompatibility();
                    data.setEnabled(true);
                    foreignKeys.setEnabled(true);
                    indices.setEnabled(true);
                    quotedIdentifiers.setEnabled(true);
                    break;
                case XML:
                    compatibilitySelect.setEnabled(false);
                    compatibilitySelect.setEmptySelectionAllowed(true);
                    compatibilitySelect.setValue(null);
                    data.setEnabled(true);
                    foreignKeys.setEnabled(true);
                    indices.setEnabled(true);
                    quotedIdentifiers.setEnabled(true);
                    break;
                case CSV:
                case CSV_DQUOTE:
                case SYM_XML:
                    compatibilitySelect.setEnabled(false);
                    compatibilitySelect.setEmptySelectionAllowed(true);
                    compatibilitySelect.setValue(null);
                    data.setEnabled(false);
                    foreignKeys.setEnabled(false);
                    indices.setEnabled(false);
                    quotedIdentifiers.setEnabled(false);
            }
        });
        formatSelect.setSelectedItem(DbExportFormat.SQL);
        formLayout.addComponent(formatSelect);

        compatibilitySelect = new ComboBox<Compatible>("Compatibility", Arrays.asList(Compatible.values()));

        compatibilitySelect.setEmptySelectionAllowed(false);
        setDefaultCompatibility();
        formLayout.addComponent(compatibilitySelect);

        createInfo = new CheckBox("Create Tables");
        formLayout.addComponent(createInfo);

        dropTables = new CheckBox("Drop Tables");
        formLayout.addComponent(dropTables);

        data = new CheckBox("Insert Data");
        data.setValue(true);
        formLayout.addComponent(data);

        foreignKeys = new CheckBox("Create Foreign Keys");
        formLayout.addComponent(foreignKeys);

        indices = new CheckBox("Create Indices");
        formLayout.addComponent(indices);

        quotedIdentifiers = new CheckBox("Qualify with Quoted Identifiers");
        formLayout.addComponent(quotedIdentifiers);

        whereClauseField = new TextArea("Where Clause");
        whereClauseField.setWidth(100, Unit.PERCENTAGE);
        whereClauseField.setRows(2);
        formLayout.addComponent(whereClauseField);

        exportFormatOptionGroup = new RadioButtonGroup<String>("Export Format");
        List<String> formatList = new ArrayList<String>();
        formatList.add(EXPORT_AS_A_FILE);
        if (queryPanel != null) {
           formatList.add(EXPORT_TO_THE_SQL_EDITOR);
        }
        exportFormatOptionGroup.setItems(formatList);
        exportFormatOptionGroup.setValue(EXPORT_AS_A_FILE);
        exportFormatOptionGroup.addValueChangeListener(event -> setExportButtonsEnabled());
        formLayout.addComponent(exportFormatOptionGroup);

    }

    protected void setDefaultCompatibility() {
        try {
            compatibilitySelect.setValue(Compatible.valueOf(databasePlatform.getName()
                    .toUpperCase()));
        } catch (Exception ex) {
            compatibilitySelect.setValue(Compatible.ORACLE);
        }
    }

    protected void setExportButtonsEnabled() {
        if (exportFormatOptionGroup.getValue().equals(EXPORT_AS_A_FILE)) {
            exportEditorButton.setVisible(false);
            exportFileButton.setVisible(true);
            exportFileButton.setClickShortcut(KeyCode.ENTER);
            exportFileButton.focus();
        } else {
            exportFileButton.setVisible(false);
            exportEditorButton.setVisible(true);
            exportEditorButton.setClickShortcut(KeyCode.ENTER);
            exportEditorButton.focus();
        }
        doneButton.setVisible(true);
        cancelButton.setVisible(false);
    }

    protected void previous() {
        content.removeComponent(optionLayout);
        content.addComponent(tableSelectionLayout, 0);
        content.setExpandRatio(tableSelectionLayout, 1);
        previousButton.setVisible(false);
        exportEditorButton.setVisible(false);
        exportEditorButton.removeClickShortcut();
        exportFileButton.setVisible(false);
        exportFileButton.removeClickShortcut();
        doneButton.setVisible(false);
        doneButton.removeClickShortcut();
        nextButton.setVisible(true);
        nextButton.setClickShortcut(KeyCode.ENTER);
        nextButton.focus();
        selectAllLink.setVisible(true);
        selectNoneLink.setVisible(true);
        cancelButton.setVisible(true);
    }

    protected void next() {
        content.removeComponent(tableSelectionLayout);
        content.addComponent(optionLayout, 0);
        content.setExpandRatio(optionLayout, 1);
        nextButton.setVisible(false);
        nextButton.removeClickShortcut();
        previousButton.setVisible(true);
        setExportButtonsEnabled();
        selectAllLink.setVisible(false);
        selectNoneLink.setVisible(false);

    }

    protected void createDbExport() {
        dbExport = new DbExport(databasePlatform);
        if (tableSelectionLayout.schemaSelect.getValue() != null) {
            dbExport.setSchema(tableSelectionLayout.schemaSelect.getValue().toString());
        }
        if (tableSelectionLayout.catalogSelect.getValue() != null) {
            dbExport.setCatalog(tableSelectionLayout.catalogSelect.getValue().toString());
        }
        dbExport.setNoCreateInfo(!createInfo.getValue());
        dbExport.setNoData(!data.getValue());
        dbExport.setNoForeignKeys(!foreignKeys.getValue());
        dbExport.setNoIndices(!indices.getValue());
        dbExport.setAddDropTable(dropTables.getValue());
        Format format = DbExport.Format.valueOf(formatSelect.getValue().toString());
        dbExport.setFormat(format);
        if (compatibilitySelect.getValue() != null) {
            Compatible compatible = (Compatible) compatibilitySelect.getValue();
            dbExport.setCompatible(compatible);
        }
        dbExport.setUseQuotedIdentifiers(quotedIdentifiers.getValue());
        dbExport.setWhereClause(whereClauseField.getValue());
    }

    protected void exportToEditor() {
        List<String> list = tableSelectionLayout.getSelectedTables();
        String[] array = new String[list.size()];
        list.toArray(array);

        createDbExport();

        String script;
        try {
            script = dbExport.exportTables(array);
            queryPanel.appendSql(script);
        } catch (IOException e) {
            String msg = "Failed to export to the sql editor";
            log.error(msg, e);
            CommonUiUtils.notify(msg, e);
        }
    }

    protected void buildFileDownloader() {
        if (fileDownloader != null) {
            fileDownloader.remove();
        }
        fileDownloader = new FileDownloader(createResource()) {
            private static final long serialVersionUID = 1L;
        };
        fileDownloader.extend(exportFileButton);
    }

    private StreamResource createResource() {

        String format = (String) formatSelect.getValue().toString();
        if (format.equals("CSV_DQUOTE")) {
            format = "CSV";
        }
        StreamSource ss = new StreamSource() {
            private static final long serialVersionUID = 1L;

            public InputStream getStream() {

                List<String> list = tableSelectionLayout.getSelectedTables();
                String[] array = new String[list.size()];
                list.toArray(array);

                createDbExport();

                String script;
                try {
                    script = dbExport.exportTables(array);
                    return new ByteArrayInputStream(script.getBytes());
                } catch (IOException e) {
                    String msg = "Failed to export to a file";
                    log.error(msg, e);
                    CommonUiUtils.notify(msg, e);
                }
                return null;
            }
        };
        String datetime = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        StreamResource sr = new StreamResource(ss, String.format(
                "table-export-%s." + format.toLowerCase(), datetime));
        return sr;
    }
}
