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

import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.*;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_DELIMITER;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_EXCLUDE_TABLES_REGEX;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_MAX_RESULTS;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_RESULT_AS_TEXT;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_SHOW_ROW_NUMBERS;

import java.text.DecimalFormat;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.common.ResizableWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.data.Binder;
import com.vaadin.data.converter.StringToIntegerConverter;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.ui.AbstractLayout;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

public class SettingsDialog extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private TextField rowsToFetchField;
    
    private Binder<Integer> binder;

    private CheckBox autoCommitBox;

    private CheckBox autoCompleteBox;

    private TextField delimiterField;

    private TextField excludeTablesWithPrefixField;

    private CheckBox resultAsTextBox;

    private CheckBox ignoreErrorsWhenRunningScript;

    private CheckBox showRowNumbersBox;

    private CheckBox showResultsInNewTabsBox;

    ISettingsProvider settingsProvider;

    SqlExplorer explorer;

    public SettingsDialog(SqlExplorer explorer) {
        super("Settings");
        this.explorer = explorer;
        this.settingsProvider = explorer.getSettingsProvider();
        setWidth(600, Unit.PIXELS);
        addComponent(createSettingsLayout(), 1);
        addComponent(createButtonLayout());
    }

    protected AbstractLayout createSettingsLayout() {
        GridLayout layout = new GridLayout(2, 9);
        layout.setWidth(700, Unit.PIXELS);
        layout.setMargin(new MarginInfo(false, true, false, true));
        FormLayout settingsLayout = new FormLayout();

        Settings settings = settingsProvider.get();
        TypedProperties properties = settings.getProperties();

        rowsToFetchField = new TextField("Max Results");
        rowsToFetchField.setWidth(6, Unit.EM);
        Label rowsToFetchLabel = new Label();
        rowsToFetchLabel.addStyleName("v-label-marked");
        binder = new Binder<Integer>();
        binder.forField(rowsToFetchField).withConverter(new StringToIntegerConverter("Could not convert value to Integer"))
                .withValidator(value -> value != null, "Invalid value").withValidationStatusHandler(event -> {
                    rowsToFetchLabel.setValue(event.getMessage().orElse(""));
                    if (event.isError()) {
                        rowsToFetchField.addStyleName("v-textfield-error");
                    } else {
                        rowsToFetchField.removeStyleName("v-textfield-error");
                    }
                }).bind(integer -> integer, (integer, value) -> integer = value);
        rowsToFetchField.setValue(properties.getProperty(SQL_EXPLORER_MAX_RESULTS, "100"));
        settingsLayout.addComponent(rowsToFetchField);

        delimiterField = new TextField("Delimiter");
        delimiterField.setValue(properties.getProperty(SQL_EXPLORER_DELIMITER, ";"));
        settingsLayout.addComponent(delimiterField);

        excludeTablesWithPrefixField = new TextField("Hide Tables (regex)");
        excludeTablesWithPrefixField.setValue(properties.getProperty(SQL_EXPLORER_EXCLUDE_TABLES_REGEX));
        settingsLayout.addComponent(excludeTablesWithPrefixField);

        resultAsTextBox = new CheckBox("Result As Text");
        String resultAsTextValue = (properties.getProperty(SQL_EXPLORER_RESULT_AS_TEXT, "false"));
        if (resultAsTextValue.equals("true")) {
            resultAsTextBox.setValue(true);
        } else {
            resultAsTextBox.setValue(false);
        }
        settingsLayout.addComponent(resultAsTextBox);

        ignoreErrorsWhenRunningScript = new CheckBox("Ignore Errors When Running Scripts");
        String ignoreErrorsWhenRunningScriptTextValue = (properties.getProperty(SQL_EXPLORER_IGNORE_ERRORS_WHEN_RUNNING_SCRIPTS, "false"));
        if (ignoreErrorsWhenRunningScriptTextValue.equals("true")) {
            ignoreErrorsWhenRunningScript.setValue(true);
        } else {
            ignoreErrorsWhenRunningScript.setValue(false);
        }
        settingsLayout.addComponent(ignoreErrorsWhenRunningScript);

        autoCommitBox = new CheckBox("Auto Commit");
        String autoCommitValue = (properties.getProperty(SQL_EXPLORER_AUTO_COMMIT, "true"));
        if (autoCommitValue.equals("true")) {
            autoCommitBox.setValue(true);
        } else {
            autoCommitBox.setValue(false);
        }
        settingsLayout.addComponent(autoCommitBox);

        autoCompleteBox = new CheckBox("Auto Complete");
        String autoCompleteValue = (properties.getProperty(SQL_EXPLORER_AUTO_COMPLETE, "true"));
        if (autoCompleteValue.equals("true")) {
            autoCompleteBox.setValue(true);
        } else {
            autoCompleteBox.setValue(false);
        }
        settingsLayout.addComponent(autoCompleteBox);

        showRowNumbersBox = new CheckBox("Show Row Numbers");
        String showRowNumbersValue = (properties.getProperty(SQL_EXPLORER_SHOW_ROW_NUMBERS, "true"));
        if (showRowNumbersValue.equals("true")) {
            showRowNumbersBox.setValue(true);
        } else {
            showRowNumbersBox.setValue(false);
        }
        settingsLayout.addComponent(showRowNumbersBox);

        showResultsInNewTabsBox = new CheckBox("Always Put Results In New Tabs");
        String showResultsInNewTabsValue = (properties.getProperty(SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS, "false"));
        if (showResultsInNewTabsValue.equals("true")) {
            showResultsInNewTabsBox.setValue(true);
        } else {
            showResultsInNewTabsBox.setValue(false);
        }
        settingsLayout.addComponent(showResultsInNewTabsBox);

        layout.addComponent(settingsLayout, 0, 0, 0, 8);
        layout.addComponent(rowsToFetchLabel, 1, 0);
        layout.setComponentAlignment(rowsToFetchLabel, Alignment.MIDDLE_LEFT);
        
        return layout;

    }

    protected AbstractLayout createButtonLayout() {
        Button saveButton = CommonUiUtils.createPrimaryButton("Save", new Button.ClickListener() {
            private static final long serialVersionUID = 1L;

            public void buttonClick(ClickEvent event) {
                if (save()) {
                    UI.getCurrent().removeWindow(SettingsDialog.this);
                }
            }
        });

        return buildButtonFooter(new Button("Cancel", new CloseButtonListener()), saveButton);
    }

    protected boolean save() {
        if (binder.validate().isOk()) {
            Settings settings = settingsProvider.get();
            TypedProperties properties = settings.getProperties();
            
            try {
                properties.setProperty(SQL_EXPLORER_MAX_RESULTS, new DecimalFormat().parse(rowsToFetchField.getValue()).intValue());
                properties.setProperty(SQL_EXPLORER_AUTO_COMMIT, String.valueOf(autoCommitBox.getValue()));
                properties.setProperty(SQL_EXPLORER_AUTO_COMPLETE, String.valueOf(autoCompleteBox.getValue()));
                properties.setProperty(SQL_EXPLORER_DELIMITER, delimiterField.getValue());
                properties.setProperty(SQL_EXPLORER_RESULT_AS_TEXT, String.valueOf(resultAsTextBox.getValue()));
                properties.setProperty(SQL_EXPLORER_SHOW_ROW_NUMBERS, String.valueOf(showRowNumbersBox.getValue()));
                properties.setProperty(SQL_EXPLORER_EXCLUDE_TABLES_REGEX, excludeTablesWithPrefixField.getValue());
                properties.setProperty(SQL_EXPLORER_IGNORE_ERRORS_WHEN_RUNNING_SCRIPTS, String.valueOf(ignoreErrorsWhenRunningScript.getValue()));
                properties.setProperty(SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS, String.valueOf(showResultsInNewTabsBox.getValue()));
                settingsProvider.save(settings);
                explorer.refreshQueryPanels();
                return true;
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
                CommonUiUtils.notify(ex);
                return false;
            }
        }
        CommonUiUtils.notify("Save Failed", "Ensure that all fields are valid", Type.WARNING_MESSAGE);
        return false;
    }
}
