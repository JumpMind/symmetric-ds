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

import java.io.Serializable;

import com.vaadin.server.FontAwesome;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;

public class DefaultButtonBar implements IButtonBar, Serializable {

    private static final long serialVersionUID = 1L;

    MenuItem executeAtCursorButton;

    MenuItem executeScriptButton;

    MenuItem commitButton;

    MenuItem rollbackButton;

    MenuItem databaseExplorerButton;

    MenuItem historyButton;

    MenuItem settingsButton;

    MenuItem importButton;

    MenuItem exportButton;

    MenuItem fillButton;
    
    IDbMenuItem[] additionalMenuItems;
    
    QueryPanel queryPanel;
    
    ISettingsProvider settingsProvider;
    
    IDb db;
    
    protected void init(IDb db, ISettingsProvider settingsProvider, QueryPanel queryPanel, IDbMenuItem... additionalMenuItems) {
        this.db = db;
        this.settingsProvider = settingsProvider;
        this.queryPanel = queryPanel;
        this.additionalMenuItems = additionalMenuItems;
    }
    
    @Override
    public void setCommitButtonEnabled(boolean enabled) {
        commitButton.setEnabled(enabled);
    }
    
    @Override
    public void setExecuteAtCursorButtonEnabled(boolean enabled) {
        executeAtCursorButton.setEnabled(enabled);
    }
    
    @Override
    public void setExecuteScriptButtonEnabled(boolean enabled) {
        executeScriptButton.setEnabled(enabled);
    }
    
    @Override
    public void setRollbackButtonEnabled(boolean enabled) {
        rollbackButton.setEnabled(enabled);
    }
    
    protected void populate(MenuBar menuBar) {
        executeAtCursorButton = menuBar.addItem("", FontAwesome.PLAY, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                queryPanel.execute(false);
            }
        });
        executeAtCursorButton.setDescription("Run sql under cursor (CTRL+ENTER)");

        executeScriptButton = menuBar.addItem("", FontAwesome.FORWARD, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                queryPanel.execute(true);
            }
        });
        executeScriptButton.setDescription("Run as script");

        commitButton = menuBar.addItem("", FontAwesome.ARROW_CIRCLE_O_RIGHT, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                queryPanel.commit();
            }
        });
        commitButton.setStyleName("green");
        commitButton.setDescription("Commit");
        commitButton.setEnabled(false);

        rollbackButton = menuBar.addItem("", FontAwesome.ARROW_CIRCLE_O_LEFT, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                queryPanel.rollback();
            }
        });
        rollbackButton.setStyleName("red");
        rollbackButton.setDescription("Rollback");
        rollbackButton.setEnabled(false);

        historyButton = menuBar.addItem("", FontAwesome.SEARCH, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                new SqlHistoryDialog(settingsProvider, queryPanel).showAtSize(0.6);
            }
        });
        historyButton.setDescription("Sql History");
        historyButton.setEnabled(true);

        MenuItem optionsButton = menuBar.addItem("", FontAwesome.TASKS, null);
        optionsButton.setDescription("Options");

        importButton = optionsButton.addItem("DB Import", FontAwesome.DOWNLOAD, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                new DbImportDialog(db.getPlatform()).showAtSize(0.6);
            }
        });

        exportButton = optionsButton.addItem("DB Export", FontAwesome.UPLOAD, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                new DbExportDialog(db.getPlatform(), queryPanel).showAtSize(0.6);
            }
        });
        
        fillButton = optionsButton.addItem("DB Fill", FontAwesome.BEER, new Command() {

            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                new DbFillDialog(db.getPlatform(), queryPanel).showAtSize(0.6);
            }
        });

        for (IDbMenuItem item : additionalMenuItems) {
        	optionsButton.addItem(item.getCaption(), item.getIcon(), item.getCommand());
        }
    }
}
