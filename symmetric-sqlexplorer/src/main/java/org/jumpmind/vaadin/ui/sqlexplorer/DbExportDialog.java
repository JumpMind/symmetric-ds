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
import org.jumpmind.vaadin.ui.common.ResizableDialog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.ShortcutRegistration;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.Anchor;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.radiobutton.RadioButtonGroup;
import com.vaadin.flow.component.textfield.TextArea;
import com.vaadin.flow.server.StreamResource;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class DbExportDialog extends ResizableDialog {

    private static final String EXPORT_TO_THE_SQL_EDITOR = "Export to the SQL Editor";

    private static final String EXPORT_AS_A_FILE = "Export as a File";

    private static final long serialVersionUID = 1L;

    final Logger log = LoggerFactory.getLogger(getClass());

    enum DbExportFormat {
        SQL, XML, CSV, SYM_XML, CSV_DQUOTE;
    }
    
    private ComboBox<DbExportFormat> formatSelect;

    private ComboBox<Compatible> compatibilitySelect;

    private Checkbox data;

    private Checkbox createInfo;

    private Checkbox foreignKeys;

    private Checkbox indices;

    private Checkbox quotedIdentifiers;

    private Checkbox dropTables;

    private TextArea whereClauseField;

    private Button previousButton;

    private Button cancelButton;

    private Button selectAllLink;

    private Button selectNoneLink;

    public Button nextButton;
    
    private ShortcutRegistration nextShortcutRegistration;

    private Button exportFileButton;
    
    private ShortcutRegistration exportFileShortcutRegistration;

    private Button exportEditorButton;
    
    private ShortcutRegistration exportEditorShortcutRegistration;
    
    private Button doneButton;
    
    private ShortcutRegistration doneShortcutRegistration;

    private TableSelectionLayout tableSelectionLayout;

    private VerticalLayout optionLayout;

    private Anchor fileDownloader;

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

        add(tableSelectionLayout, 1);

        addButtons();
        
        nextShortcutRegistration = nextButton.addClickShortcut(Key.ENTER);
        nextButton.focus();
    }

    protected void addButtons() {
        selectAllLink = new Button("Select All");
        selectAllLink.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_TERTIARY_INLINE);
        selectAllLink.addClickListener(event -> tableSelectionLayout.selectAll());

        selectNoneLink = new Button("Select None");
        selectNoneLink.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_TERTIARY_INLINE);
        selectNoneLink.addClickListener(event -> tableSelectionLayout.selectNone());

        nextButton = CommonUiUtils.createPrimaryButton("Next");
        nextButton.setEnabled(tableSelectionLayout.getSelectedTables().size() > 0);
        nextButton.addClickListener(event -> next());

        cancelButton = new Button("Cancel", event -> close());

        previousButton = new Button("Previous", event -> previous());
        previousButton.setVisible(false);

        exportFileButton = CommonUiUtils.createPrimaryButton("Export", event -> {
            exportFileShortcutRegistration.remove();
            fileDownloader.setHref(createResource());
            doneShortcutRegistration = doneButton.addClickShortcut(Key.ENTER);
            doneButton.focus();
        });
        buildFileDownloader();
        fileDownloader.setVisible(false);

        exportEditorButton = CommonUiUtils.createPrimaryButton("Export", event -> {
            exportToEditor();
            close();
        });
        exportEditorButton.setVisible(false);
        
        doneButton = new Button("Close", event -> close());
        doneButton.setVisible(false);

        add(buildButtonFooter(new Button[] { selectAllLink, selectNoneLink },
                cancelButton, previousButton, nextButton, fileDownloader, exportEditorButton, doneButton));

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

        formatSelect = new ComboBox<DbExportFormat>("Format", Arrays.asList(DbExportFormat.values()));
        formatSelect.setValue(DbExportFormat.SQL);
        formatSelect.addValueChangeListener(event -> {
            switch (formatSelect.getValue()) {
                case SQL:
                    compatibilitySelect.setEnabled(true);
                    setDefaultCompatibility();
                    data.setEnabled(true);
                    foreignKeys.setEnabled(true);
                    indices.setEnabled(true);
                    quotedIdentifiers.setEnabled(true);
                    break;
                case XML:
                    compatibilitySelect.setEnabled(false);
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
                    compatibilitySelect.setValue(null);
                    data.setEnabled(false);
                    foreignKeys.setEnabled(false);
                    indices.setEnabled(false);
                    quotedIdentifiers.setEnabled(false);
            }
        });
        formatSelect.setValue(DbExportFormat.SQL);
        formLayout.add(formatSelect);

        compatibilitySelect = new ComboBox<Compatible>("Compatibility", Arrays.asList(Compatible.values()));

        setDefaultCompatibility();
        formLayout.add(compatibilitySelect);

        createInfo = new Checkbox("Create Tables");
        formLayout.add(createInfo);

        dropTables = new Checkbox("Drop Tables");
        formLayout.add(dropTables);

        data = new Checkbox("Insert Data");
        data.setValue(true);
        formLayout.add(data);

        foreignKeys = new Checkbox("Create Foreign Keys");
        formLayout.add(foreignKeys);

        indices = new Checkbox("Create Indices");
        formLayout.add(indices);

        quotedIdentifiers = new Checkbox("Qualify with Quoted Identifiers");
        formLayout.add(quotedIdentifiers);

        whereClauseField = new TextArea("Where Clause");
        whereClauseField.setWidthFull();
        formLayout.add(whereClauseField);

        exportFormatOptionGroup = new RadioButtonGroup<String>();
        exportFormatOptionGroup.setLabel("Export Format");
        List<String> formatList = new ArrayList<String>();
        formatList.add(EXPORT_AS_A_FILE);
        if (queryPanel != null) {
           formatList.add(EXPORT_TO_THE_SQL_EDITOR);
        }
        exportFormatOptionGroup.setItems(formatList);
        exportFormatOptionGroup.setValue(EXPORT_AS_A_FILE);
        exportFormatOptionGroup.addValueChangeListener(event -> setExportButtonsEnabled());
        formLayout.add(exportFormatOptionGroup);

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
            fileDownloader.setVisible(true);
            exportFileShortcutRegistration = exportFileButton.addClickShortcut(Key.ENTER);
            exportFileButton.focus();
        } else {
            fileDownloader.setVisible(false);
            exportEditorButton.setVisible(true);
            exportEditorShortcutRegistration = exportEditorButton.addClickShortcut(Key.ENTER);
            exportEditorButton.focus();
        }
        doneButton.setVisible(true);
        cancelButton.setVisible(false);
    }

    protected void previous() {
        content.remove(optionLayout);
        content.addComponentAtIndex(0, tableSelectionLayout);
        content.expand(tableSelectionLayout);
        previousButton.setVisible(false);
        exportEditorButton.setVisible(false);
        if (exportEditorShortcutRegistration != null) {
            exportEditorShortcutRegistration.remove();
        }
        fileDownloader.setVisible(false);
        if (exportFileShortcutRegistration != null) {
            exportFileShortcutRegistration.remove();
        }
        doneButton.setVisible(false);
        if (doneShortcutRegistration != null) {
            doneShortcutRegistration.remove();
        }
        nextButton.setVisible(true);
        nextButton.addClickShortcut(Key.ENTER);
        nextButton.focus();
        selectAllLink.setVisible(true);
        selectNoneLink.setVisible(true);
        cancelButton.setVisible(true);
    }

    protected void next() {
        content.remove(tableSelectionLayout);
        content.addComponentAtIndex(0, optionLayout);
        content.expand(optionLayout);
        nextButton.setVisible(false);
        nextShortcutRegistration.remove();
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
            //queryPanel.appendSql(script);
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
        fileDownloader = new Anchor(createResource(), null);
        fileDownloader.getElement().setAttribute("download", true);
        fileDownloader.add(exportFileButton);
    }

    private StreamResource createResource() {

        String format = (String) formatSelect.getValue().toString();
        if (format.equals("CSV_DQUOTE")) {
            format = "CSV";
        }
        String datetime = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        StreamResource sr = new StreamResource(String.format("table-export-%s." + format.toLowerCase(), datetime), () -> {
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
                });
        return sr;
    }
}
