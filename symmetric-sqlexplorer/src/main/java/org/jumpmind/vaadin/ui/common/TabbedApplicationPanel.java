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

import java.util.Iterator;

import com.vaadin.server.Resource;
import com.vaadin.ui.Component;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.themes.ValoTheme;

public class TabbedApplicationPanel extends TabSheet {

    private static final long serialVersionUID = 1L;

    protected Tab mainTab;
    
    public TabbedApplicationPanel() {
        setSizeFull();
        addStyleName(ValoTheme.TABSHEET_FRAMED);
        addStyleName(ValoTheme.TABSHEET_COMPACT_TABBAR);

        addSelectedTabChangeListener(new SelectedTabChangeListener() {
            private static final long serialVersionUID = 1L;        
            @Override
            public void selectedTabChange(SelectedTabChangeEvent event) {
                Component selected = event.getTabSheet().getSelectedTab();
                if (selected instanceof IUiPanel) {
                    ((IUiPanel)selected).selected();
                }
            }
        });
        
        setCloseHandler(new CloseHandler() {            
            private static final long serialVersionUID = 1L;
            @Override
            public void onTabClose(TabSheet tabsheet, Component tabContent) {
                if (tabContent instanceof IUiPanel) {
                    if (((IUiPanel)tabContent).closing()) {
                        tabsheet.removeComponent(tabContent);
                    }
                } else {
                    tabsheet.removeComponent(tabContent);
                }
            }
        });
    }

    public void setMainTab(String caption, Resource icon, Component component) {
        component.setSizeFull();
        this.mainTab = addTab(component, caption, icon, 0);
    }

    public void addCloseableTab(String caption, Resource icon, Component component) {
        Iterator<Component> i = iterator();
        while (i.hasNext()) {
            Component c = i.next();
            if (getTab(c).getCaption().equals(caption)) {
                setSelectedTab(c);
                return;
            }
        } 
        
        component.setSizeFull();
        Tab tab = addTab(component, caption, icon, mainTab == null ? 0 : 1);
        tab.setClosable(true);
        setSelectedTab(tab);
    }

}
