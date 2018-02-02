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
package org.jumpmind.vaadin.ui.common;

import java.util.Arrays;
import java.util.List;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.RadioButtonGroup;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

public class ExportDialog extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    VerticalLayout exportLayout;

    RadioButtonGroup<String> oGroup;

    Button cancelButton;

    Button exportButton;

    Object target;

    String filePrefix;

    String reportTitle;

    public ExportDialog(String filePrefix, String reportTitle) {
        super("Export");
        this.filePrefix = filePrefix;
        this.reportTitle = reportTitle;

        setModal(true);
        setWidth(300, Unit.PIXELS);
        setHeight(180, Unit.PIXELS);
        setClosable(true);

        createExportLayout();
        setContent(exportLayout);
    }

    public ExportDialog(IDataProvider grid, String filePrefix, String reportTitle) {
        this(filePrefix, reportTitle);
        target = grid;
    }

    protected void createExportLayout() {
        exportLayout = new VerticalLayout();
        exportLayout.setSizeFull();
        exportLayout.setMargin(true);
        exportLayout.setSpacing(true);

        HorizontalLayout exportOptionsLayout = new HorizontalLayout();
        exportOptionsLayout.setSpacing(true);
        Label optionGroupLabel = new Label("Export Format <span style='padding-left:0px; color: red'>*</span>", ContentMode.HTML);
        exportOptionsLayout.addComponent(optionGroupLabel);

        oGroup = new RadioButtonGroup<String>();
        List<String> options = Arrays.asList("CSV", "Excel");
        oGroup.setItems(options);
        oGroup.setValue("CSV");

        exportOptionsLayout.addComponent(oGroup);
        exportLayout.addComponent(exportOptionsLayout);
        exportLayout.setExpandRatio(exportOptionsLayout, 1);

        cancelButton = new Button("Cancel", new Button.ClickListener() {
            static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                UI.getCurrent().removeWindow(ExportDialog.this);
            }
        });

        exportButton = CommonUiUtils.createPrimaryButton("Export", new Button.ClickListener() {
            static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                if (oGroup.getValue().toString().equals("CSV")) {
                    csvExport();
                } else {
                    excelExport();
                }
                UI.getCurrent().removeWindow(ExportDialog.this);
            }
        });
        exportButton.setClickShortcut(KeyCode.ENTER);

        exportLayout.addComponent(buildButtonFooter(cancelButton, exportButton));

    }

    protected void csvExport() {
        CsvExport csvExport = null;
        if (target instanceof IDataProvider) {
            csvExport = new CsvExport((IDataProvider) target);
            csvExport.setFileName(filePrefix + "-export.csv");
            csvExport.setTitle(reportTitle);
            csvExport.export();
        }

    }

    protected void excelExport() {
        ExcelExport excelExport = null;
        if (target instanceof IDataProvider) {
            excelExport = new ExcelExport((IDataProvider) target);
            excelExport.setFileName(filePrefix + "-export.xls");
            excelExport.setTitle(reportTitle);
            excelExport.export();
        }
    }
}
