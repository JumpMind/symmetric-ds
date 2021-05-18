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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_AUTO_COMMIT;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_AUTO_COMPLETE;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_DELIMITER;
import static org.jumpmind.vaadin.ui.sqlexplorer.Settings.SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS;

import java.io.Serializable;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vaadin.aceeditor.AceEditor;
import org.vaadin.aceeditor.AceEditor.SelectionChangeEvent;
import org.vaadin.aceeditor.AceEditor.SelectionChangeListener;
import org.vaadin.aceeditor.AceMode;
import org.vaadin.aceeditor.Suggester;
import org.vaadin.aceeditor.Suggestion;
import org.vaadin.aceeditor.SuggestionExtension;
import org.vaadin.aceeditor.TextRange;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.ShortcutRegistration;
import com.vaadin.flow.component.orderedlayout.FlexComponent.Alignment;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.splitlayout.SplitLayout;

public class QueryPanel extends SplitLayout implements IContentTab {

    protected static final Logger log = LoggerFactory.getLogger(QueryPanel.class);

    private static final long serialVersionUID = 1L;

    //AceEditor sqlArea;

    IDb db;

    SelectionChangeListener selectionChangeListener;

    List<ShortcutRegistration> shortcutRegistrations;

    boolean executeAtCursorButtonValue = false;

    boolean executeScriptButtonValue = false;

    boolean commitButtonValue = false;

    boolean rollbackButtonValue = false;

    IButtonBar buttonBar;

    //TabSheet resultsTabs;

    //Tab errorTab;

    int maxNumberOfResultTabs = 10;

    ISettingsProvider settingsProvider;

    String user;

    Connection connection;

    Span status;

    SqlSuggester suggester;

    boolean canceled = false;

    VerticalLayout emptyResults;

    Map<Component, String> resultStatuses;

    //Tab generalResultsTab;

    //private SuggestionExtension suggestionExtension;

    //private AceEditor editor;

    transient Set<SqlRunner> runnersInProgress = new HashSet<SqlRunner>();

    public QueryPanel(IDb db, ISettingsProvider settingsProvider, IButtonBar buttonBar, String user) {
        this.settingsProvider = settingsProvider;
        this.db = db;
        this.user = user;
        this.buttonBar = buttonBar;
        //this.sqlArea = buildSqlEditor();
        this.shortcutRegistrations = new ArrayList<ShortcutRegistration>();
        
        this.setOrientation(Orientation.VERTICAL);

        VerticalLayout resultsLayout = new VerticalLayout();
        resultsLayout.setMargin(false);
        resultsLayout.setSpacing(false);
        resultsLayout.setSizeFull();

        //resultsTabs = CommonUiUtils.createTabSheet();
        resultStatuses = new HashMap<Component, String>();

        HorizontalLayout statusBar = new HorizontalLayout();
        statusBar.setMargin(false);
        //statusBar.addClassName(ValoTheme.PANEL_WELL);
        statusBar.setWidthFull();

        status = new Span("No Results");
        status.getElement().setAttribute("theme", "font-size-s");
        statusBar.add(status);

        setSelectedTabChangeListener();

        //resultsLayout.add(resultsTabs, statusBar);
        //resultsLayout.expand(resultsTabs);

        //addToPrimary(sqlArea);
        addToSecondary(resultsLayout);

        setSplitterPosition(50);

        emptyResults = new VerticalLayout();
        emptyResults.setSizeFull();
        Span span = new Span("New results will appear here");
        span.setWidth(null);
        emptyResults.add(span);
        emptyResults.setHorizontalComponentAlignment(Alignment.CENTER, span);
        resultStatuses.put(emptyResults, "No Results");

        if (!settingsProvider.get().getProperties().is(SQL_EXPLORER_SHOW_RESULTS_IN_NEW_TABS)) {
            createGeneralResultsTab();
        }
    }

    public IDb getDb() {
        return db;
    }

    /*protected AceEditor buildSqlEditor() {
        editor = CommonUiUtils.createAceEditor();
        editor.setMode(AceMode.sql);
        editor.addValueChangeListener(event -> {
            if (!editor.getValue().equals("")) {
                executeAtCursorButtonValue = true;
                executeScriptButtonValue = true;
            } else {
                executeAtCursorButtonValue = false;
                executeScriptButtonValue = false;
            }
            setButtonsEnabled();
        });

        boolean autoSuggestEnabled = settingsProvider.get().getProperties().is(SQL_EXPLORER_AUTO_COMPLETE);
        setAutoCompleteEnabled(autoSuggestEnabled);

        selectionChangeListener = new DummyChangeListener();
        return editor;
    }*/

    public IButtonBar getButtonBar() {
        return buttonBar;
    }

    protected void setSelectedTabChangeListener() {
        /*resultsTabs.addSelectedTabChangeListener(new SelectedTabChangeListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void selectedTabChange(SelectedTabChangeEvent event) {
                Component tab = resultsTabs.getSelectedTab();
                String st = resultStatuses.get(tab);
                if (st == null && tab instanceof VerticalLayout) {
                    if (((VerticalLayout) tab).getComponentCount() > 0) {
                        st = resultStatuses.get(((VerticalLayout) tab).getComponent(0));
                    }
                }
                if (st == null) {
                    st = "No Results";
                }
                status.setValue(st);
            }
        });*/
    }

    /*public Tab getGeneralResultsTab() {
        return generalResultsTab;
    }*/

    public void createGeneralResultsTab() {
        /*if (generalResultsTab == null) {
            VerticalLayout generalResultsPanel = new VerticalLayout();
            generalResultsPanel.setSizeFull();
            generalResultsTab = resultsTabs.addTab(generalResultsPanel, "Results", null, 0);
            resetGeneralResultsTab();
        }*/
    }

    public void removeGeneralResultsTab() {
        /*if (generalResultsTab != null) {
            Component content = ((VerticalLayout) generalResultsTab.getComponent()).getComponentAt(0);
            if (content instanceof TabularResultLayout) {
                addResultsTab(((TabularResultLayout) content).refreshWithoutSaveButton(),
                        StringUtils.abbreviate(((TabularResultLayout) content).getSql(), 20), generalResultsTab.getIcon(), 0);
            }
            resultsTabs.remove(generalResultsTab.getComponent());
            generalResultsTab = null;
        }*/
    }

    public void resetGeneralResultsTab() {
        /*if (generalResultsTab != null) {
            replaceGeneralResultsWith(emptyResults, null);
        }*/
    }

    public void replaceGeneralResultsWith(Component newComponent, VaadinIcon icon) {
        /*((VerticalLayout) generalResultsTab.getComponent()).removeAll();
        ((VerticalLayout) generalResultsTab.getComponent()).add(newComponent);
        generalResultsTab.setIcon(icon);*/
    }

    @Override
    public void selected() {
        unselected();

        /*sqlArea.addSelectionChangeListener(selectionChangeListener);
        for (ShortcutListener l : shortCutListeners.keySet()) {
            shortCutListeners.put(l, sqlArea.addShortcutListener(l));
        }*/

        setButtonsEnabled();
        //sqlArea.focus();
    }

    @Override
    public void unselected() {
        /*sqlArea.removeSelectionChangeListener(selectionChangeListener);
        for (ShortcutListener l : shortCutListeners.keySet()) {
            Registration r = shortCutListeners.get(l);
            if (r != null) {
                r.remove();
            }
        }*/
    }

    protected void setButtonsEnabled() {
        buttonBar.setExecuteScriptButtonEnabled(executeScriptButtonValue);
        buttonBar.setExecuteAtCursorButtonEnabled(executeAtCursorButtonValue);
        buttonBar.setCommitButtonEnabled(commitButtonValue);
        buttonBar.setRollbackButtonEnabled(rollbackButtonValue);
    }

    /*protected ShortcutListener createExecuteSqlShortcutListener() {
        return new ShortcutListener("", KeyCode.ENTER, new int[] { ModifierKey.CTRL }) {

            private static final long serialVersionUID = 1L;

            @Override
            public void handleAction(Object sender, Object target) {
                if (target instanceof Grid<?>) {
                    Grid<?> table = (Grid<?>) target;
                    HasComponents parent = table.getParent();
                    if (parent instanceof TabularResultLayout) {
                        TabularResultLayout layout = (TabularResultLayout) parent;
                        reExecute(layout.getSql());
                    }
                } else if (target instanceof AceEditor) {
                    if (executeAtCursorButtonValue) {
                        if (execute(false) && !settingsProvider.get().getProperties().is(SQL_EXPLORER_AUTO_COMMIT)) {
                            setButtonsEnabled();
                        }
                    }
                }
            }
        };
    }
    
    protected ShortcutListener createExecuteSqlScriptShortcutListener(){
        return new ShortcutListener("", KeyCode.ENTER, new int[] {ModifierKey.CTRL, ModifierKey.SHIFT}){
            
            private static final long serialVersionUID = 1L;
            
            @Override
            public void handleAction(Object sender, Object target){
                if (target instanceof Grid<?>) {
                    Grid<?> table = (Grid<?>) target;
                    TabularResultLayout layout = (TabularResultLayout) table.getParent();
                    reExecute(layout.getSql());
                }else if (target instanceof AceEditor){
                    if(executeScriptButtonValue){
                        if(execute(true) && !settingsProvider.get().getProperties().is(SQL_EXPLORER_AUTO_COMMIT)){
                            setButtonsEnabled();
                        }
                    }
                }
            }
        };
    }*/

    protected void addToSqlHistory(String sqlStatement, Date executeTime, long executeDuration, String userId) {
        sqlStatement = sqlStatement.trim();
        Settings settings = settingsProvider.load();
        SqlHistory history = settings.getSqlHistory(sqlStatement);
        if (history == null) {
            history = new SqlHistory();
            history.setSqlStatement(sqlStatement);
            settings.addSqlHistory(history);
        }
        history.setLastExecuteDuration(executeDuration);
        history.setExecuteCount(history.getExecuteCount() + 1);
        history.setLastExecuteUserId(userId);
        history.setLastExecuteTime(executeTime);
        settingsProvider.save(settings);
    }

    /*protected boolean reExecute(String sql) {
        Component comp = resultsTabs.getSelectedTab();
        Tab tab = resultsTabs.getTab(comp);
        int tabPosition = resultsTabs.getTabPosition(tab);
        if (generalResultsTab != null && generalResultsTab == tab) {
            return execute(false, sql, tabPosition);
        } else {
            resultsTabs.removeTab(tab);
            return execute(false, sql, tabPosition, true);
        }
    }

    public boolean execute(final boolean runAsScript) {
        return execute(runAsScript, null, resultsTabs.getComponentCount());
    }

    public void appendSql(String sql) {
        if (isNotBlank(sql)) {
            sqlArea.setValue((isNotBlank(sqlArea.getValue()) ? sqlArea.getValue() + "\n" : "") + sql);
        }
    }

    public String getSql() {
        return sqlArea.getValue();
    }

    protected void executeSql(String sql, boolean writeToQueryWindow) {
        if (writeToQueryWindow) {
            appendSql(sql);
        }
        execute(false, sql, resultsTabs.getComponentCount());
    }

    protected boolean execute(final boolean runAsScript, String sqlText, final int tabPosition) {
        return execute(runAsScript, sqlText, tabPosition, false);
    }

    protected boolean execute(final boolean runAsScript, String sqlText, final int tabPosition, final boolean forceNewTab) {
        boolean scheduled = false;
        if (runnersInProgress == null) {
            runnersInProgress = new HashSet<SqlRunner>();
        }

        if (sqlText == null) {
            if (!runAsScript) {
                sqlText = selectSqlToRun();
            } else {
                sqlText = sqlArea.getValue();
            }

            sqlText = sqlText != null ? sqlText.trim() : null;
        }

        if (StringUtils.isNotBlank(sqlText)) {

            final HorizontalLayout executingLayout = new HorizontalLayout();
            executingLayout.setMargin(true);
            executingLayout.setSizeFull();
            final Span span = new Span("Executing:\n\n" + StringUtils.abbreviate(sqlText, 250), ContentMode.PREFORMATTED);
            span.setEnabled(false);
            executingLayout.add(span);
            executingLayout.setVerticalComponentAlignment(Alignment.START, span);

            final String sql = sqlText;
            final Tab executingTab;
            if (!forceNewTab && generalResultsTab != null) {
                replaceGeneralResultsWith(executingLayout, VaadinIcons.SPINNER);
                executingTab = null;
            } else {
                executingTab = resultsTabs.addTab(executingLayout, StringUtils.abbreviate(sql, 20), VaadinIcons.SPINNER, tabPosition);
            }

            if (executingTab != null) {
                executingTab.setClosable(true);
                resultsTabs.setSelectedTab(executingTab);
            }

            final SqlRunner runner = new SqlRunner(sql, runAsScript, user, db, settingsProvider.get(), this, generalResultsTab != null);
            runnersInProgress.add(runner);
            runner.setConnection(connection);
            runner.setListener(new SqlRunner.ISqlRunnerListener() {

                private static final long serialVersionUID = 1L;

                @Override
                public void writeSql(String sql) {
                    QueryPanel.this.appendSql(sql);
                }

                @Override
                public void reExecute(String sql) {
                    QueryPanel.this.reExecute(sql);
                }

                public void finished(final VaadinIcons icon, final List<Component> results, final long executionTimeInMs,
                        final boolean transactionStarted, final boolean transactionEnded) {
                    getUI().access(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                if (transactionEnded) {
                                    transactionEnded();
                                } else if (transactionStarted) {
                                    rollbackButtonValue = true;
                                    commitButtonValue = true;
                                    setButtonsEnabled();
                                    sqlArea.setClassName("transaction-in-progress");
                                    connection = runner.getConnection();
                                }

                                addToSqlHistory(StringUtils.abbreviate(sql, 1024 * 8), runner.getStartTime(), executionTimeInMs, user);

                                for (Component resultComponent : results) {
                                    resultComponent.setSizeFull();

                                    if (forceNewTab || generalResultsTab == null || results.size() > 1) {
                                        if (resultComponent instanceof TabularResultLayout) {
                                            resultComponent = ((TabularResultLayout) resultComponent).refreshWithoutSaveButton();
                                        }
                                        addResultsTab(resultComponent, StringUtils.abbreviate(sql, 20), icon, tabPosition);
                                    } else {
                                        replaceGeneralResultsWith(resultComponent, icon);
                                        resultsTabs.setSelectedTab(generalResultsTab.getComponent());
                                    }

                                    String statusVal;
                                    if (canceled) {
                                        statusVal = "Sql canceled after " + executionTimeInMs + " ms for " + db.getName() + ".  Finished at "
                                                + SimpleDateFormat.getTimeInstance().format(new Date());
                                    } else {
                                        statusVal = "Sql executed in " + executionTimeInMs + " ms for " + db.getName() + ".  Finished at "
                                                + SimpleDateFormat.getTimeInstance().format(new Date());
                                    }
                                    status.setValue(statusVal);
                                    resultStatuses.put(resultComponent, statusVal);
                                    canceled = false;
                                }
                            } finally {
                                setButtonsEnabled();
                                if (executingTab != null) {
                                    resultsTabs.removeTab(executingTab);
                                } else if (results.size() > 1) {
                                    resetGeneralResultsTab();
                                }
                                runnersInProgress.remove(runner);
                                runner.setListener(null);
                            }
                        }
                    });

                }

            });

            final Button cancel = new Button("Cancel");
            cancel.addClickListener(new ClickListener() {
                private static final long serialVersionUID = 1L;

                @Override
                public void buttonClick(ClickEvent event) {
                    log.info("Canceling sql: " + sql);
                    span.setValue("Canceling" + span.getValue().substring(9));
                    executingLayout.remove(cancel);
                    canceled = true;
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            runner.cancel();
                        }
                    }).start();
                }
            });
            executingLayout.add(cancel);

            scheduled = true;
            runner.start();

        }
        setButtonsEnabled();
        return scheduled;
    }*/

    /*public void addResultsTab(Component resultComponent, String title, Resource icon) {
        addResultsTab(resultComponent, title, icon, resultsTabs.getComponentCount());
    }

    public void addResultsTab(Component resultComponent, String title, Resource icon, int position) {
        Tab tab = resultsTabs.addTab(resultComponent, title, icon, position);

        tab.setClosable(true);

        resultsTabs.setSelectedTab(tab.getComponent());

        if (errorTab != null) {
            resultsTabs.removeTab(errorTab);
            errorTab = null;
        }

        if (maxNumberOfResultTabs > 0 && resultsTabs.getComponentCount() > maxNumberOfResultTabs) {
            resultsTabs.removeTab(resultsTabs.getTab(resultsTabs.getComponentCount() - 1));
        }

        if (icon == VaadinIcons.STOP) {
            errorTab = tab;
        }
    }*/

    public void commit() {
        try {
            SqlRunner.commit(connection);
        } catch (Exception ex) {
            Notification.show(ex.getMessage());
        } finally {
            commitButtonValue = false;
            rollbackButtonValue = false;
            executeAtCursorButtonValue = true;
            executeScriptButtonValue = true;
            setButtonsEnabled();
            connection = null;
        }
    }

    public void transactionEnded() {
        commitButtonValue = false;
        rollbackButtonValue = false;
        executeAtCursorButtonValue = true;
        executeScriptButtonValue = true;
        setButtonsEnabled();
        connection = null;
    }

    public void rollback() {
        try {
            SqlRunner.rollback(connection);
        } catch (Exception ex) {
            Notification.show(ex.getMessage());
        } finally {
            commitButtonValue = false;
            rollbackButtonValue = false;
            executeAtCursorButtonValue = true;
            executeScriptButtonValue = true;
            setButtonsEnabled();
            connection = null;
        }
    }

    /*protected String selectSqlToRun() {
        String delimiter = settingsProvider.get().getProperties().get(SQL_EXPLORER_DELIMITER);
        String sql = sqlArea.getValue();
        TextRange range = sqlArea.getSelection();
        boolean selected = !range.isZeroLength();
        if (selected) {
            if (range.isBackwards()) {
                if (sql.length() > range.getStart()) {
                    sql = sql.substring(range.getEnd(), range.getStart());
                } else {
                    selected = false;
                }

            } else {
                if (sql.length() > range.getEnd()) {
                    sql = sql.substring(range.getStart(), range.getEnd());
                } else {
                    selected = false;
                }
            }
        }

        if (!selected) {
            StringBuilder sqlBuffer = new StringBuilder();
            String[] lines = sql.split("\n");
            int charCount = 0;
            boolean pastCursor = false;
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                charCount += line.length() + (i > 0 ? 1 : 0);
                if (charCount >= sqlArea.getCursorPosition()) {
                    pastCursor = true;
                }
                if (!pastCursor) {
                    if (line.trim().endsWith(delimiter) || line.trim().equals("")) {
                        sqlBuffer.setLength(0);
                    } else {
                        sqlBuffer.append(line).append("\n");
                    }
                } else if (line.trim().endsWith(delimiter)) {
                    sqlBuffer.append(line);
                    break;
                } else if (line.trim().equals("")) {
                    break;
                } else {
                    sqlBuffer.append(line).append("\n");
                }
            }
            sql = sqlBuffer.toString();
        }
        sql = sql.trim();
        if (sql.endsWith(delimiter)) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    public void setAutoCompleteEnabled(boolean enabled) {
        if (enabled) {
            suggester = new SqlSuggester(db);
            suggestionExtension = new SuggestionExtension(suggester);
            suggestionExtension.extend(editor);
        } else if (suggestionExtension != null) {
            suggestionExtension.remove();
            BlankSuggester blank = new BlankSuggester();
            suggestionExtension = new SuggestionExtension(blank);
            suggestionExtension.extend(editor);
        }
    }

    static class DummyChangeListener implements SelectionChangeListener, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void selectionChanged(SelectionChangeEvent e) {
        }
    }
    
    static class BlankSuggester implements Suggester {

        @Override
        public List<Suggestion> getSuggestions(String text, int cursor) {
            return new ArrayList<Suggestion>();
        }

        @Override
        public String applySuggestion(Suggestion sugg, String text, int cursor) {
            return null;
        }
    }*/

}
