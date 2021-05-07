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

import com.vaadin.flow.component.contextmenu.MenuItem;
import com.vaadin.flow.component.contextmenu.SubMenu;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.menubar.MenuBar;

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
        executeAtCursorButton = menuBar.addItem(new Icon(VaadinIcon.PLAY), event -> queryPanel.execute(false));
        executeAtCursorButton.getElement().setAttribute("title", "Run sql under cursor (CTRL+ENTER)");

        executeScriptButton = menuBar.addItem(new Icon(VaadinIcon.FORWARD), event -> queryPanel.execute(true));
        executeScriptButton.getElement().setAttribute("title", "Run as script");

        commitButton = menuBar.addItem(new Icon(VaadinIcon.ARROW_CIRCLE_RIGHT_O), event -> queryPanel.commit());
        commitButton.getElement().getClassList().add("green");
        commitButton.getElement().setAttribute("title", "Commit");
        commitButton.setEnabled(false);

        rollbackButton = menuBar.addItem(new Icon(VaadinIcon.ARROW_CIRCLE_LEFT_O), event -> queryPanel.rollback());
        rollbackButton.getElement().getClassList().add("red");
        rollbackButton.getElement().setAttribute("title", "Rollback");
        rollbackButton.setEnabled(false);

        historyButton = menuBar.addItem(new Icon(VaadinIcon.SEARCH),
                event -> new SqlHistoryDialog(settingsProvider, queryPanel).showAtSize(0.6));
        historyButton.getElement().setAttribute("title", "Sql History");
        historyButton.setEnabled(true);

        MenuItem optionsButton = menuBar.addItem(new Icon(VaadinIcon.TASKS), null);
        optionsButton.getElement().setAttribute("title", "Options");

        SubMenu optionsSubMenu = optionsButton.getSubMenu();
        importButton = optionsSubMenu.addItem("DB Import", event -> new DbImportDialog(db.getPlatform()).showAtSize(0.6));
        importButton.addComponentAsFirst(new Icon(VaadinIcon.DOWNLOAD));

        exportButton = optionsSubMenu.addItem("DB Export", event -> {
            String excludeTablesRegex = settingsProvider.get().getProperties().get(Settings.SQL_EXPLORER_EXCLUDE_TABLES_REGEX);
            new DbExportDialog(db.getPlatform(), queryPanel, excludeTablesRegex).showAtSize(0.6);
        });
        exportButton.addComponentAsFirst(new Icon(VaadinIcon.UPLOAD));
        
        fillButton = optionsSubMenu.addItem("DB Fill", event -> {
            String excludeTablesRegex = settingsProvider.get().getProperties().get(Settings.SQL_EXPLORER_EXCLUDE_TABLES_REGEX);
            new DbFillDialog(db.getPlatform(), queryPanel, excludeTablesRegex).showAtSize(0.6);
        });
        fillButton.addComponentAsFirst(new Icon(VaadinIcon.FILL));

        for (IDbMenuItem item : additionalMenuItems) {
            MenuItem menuItem = optionsSubMenu.addItem(item.getCaption(), item.getListener());
            menuItem.addComponentAsFirst(item.getIcon());
        }
    }
}
