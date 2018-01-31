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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.event.ShortcutAction.ModifierKey;
import com.vaadin.event.ShortcutListener;
import com.vaadin.server.Page;
import com.vaadin.shared.ui.window.WindowMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.themes.ValoTheme;

public class ResizableWindow extends Window {

    private static final long serialVersionUID = 1L;

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected VerticalLayout content;

    public ResizableWindow() {
        this("");
    }
    
    public ResizableWindow(String caption) {
        setCaption(caption);
        setModal(true);
        setResizable(true);
        
        content = new VerticalLayout();
        content.setSizeFull();
        content.setMargin(false);
        content.setSpacing(false);
        setContent(content);
        
        addShortcutListener(new ShortcutListener("Maximize", KeyCode.M,
                new int[] { ModifierKey.CTRL }) {

            private static final long serialVersionUID = 1L;

            @Override
            public void handleAction(Object sender, Object target) {
                if (ResizableWindow.this.getWindowMode() != WindowMode.MAXIMIZED) {
                    ResizableWindow.this.setWindowMode(WindowMode.MAXIMIZED);
                } else {
                    ResizableWindow.this.setWindowMode(WindowMode.NORMAL);
                }
            }
        });
        
        addShortcutListener(new ShortcutListener("Close", KeyCode.ESCAPE, null) {

            private static final long serialVersionUID = 1L;

            @Override
            public void handleAction(Object sender, Object target) {
                close();
            }
        });
    }
    
    protected void addComponent(Component component, int expandRatio) {
        content.addComponent(component);
        content.setExpandRatio(component, expandRatio);
    }
    
    protected void addComponent(Component component) {
        content.addComponent(component);
    }
    
    protected void addComponents(Component... components) {
        for (Component component : components) {
            content.addComponent(component);    
        }        
    }

    protected Button buildCloseButton() {
        Button closeButton = new Button("Close");
        closeButton.addStyleName(ValoTheme.BUTTON_PRIMARY);
        closeButton.addClickListener(new CloseButtonListener());
        closeButton.focus();
        return closeButton;
    }
    
    protected HorizontalLayout buildButtonFooter(Button... toTheRightButtons) {
        return buildButtonFooter((Button[])null, toTheRightButtons);
    }

    protected HorizontalLayout buildButtonFooter(Button[] toTheLeftButtons, Button... toTheRightButtons) {
        HorizontalLayout footer = new HorizontalLayout();

        footer.setWidth("100%");
        footer.setSpacing(true);
        footer.addStyleName(ValoTheme.WINDOW_BOTTOM_TOOLBAR);

        if (toTheLeftButtons != null) {
            footer.addComponents(toTheLeftButtons);
        }
        
        Label footerText = new Label("");
        footerText.setSizeUndefined();

        footer.addComponents(footerText);
        footer.setExpandRatio(footerText, 1);

        if (toTheRightButtons != null) {
            footer.addComponents(toTheRightButtons);
        }


        return footer;
    }

    protected void grabFocus() {
        this.focus();
    }
    
    protected boolean onClose() { return true;}

    public void show() {
        if (!UI.getCurrent().getWindows().contains(this)) {
            UI.getCurrent().addWindow(this);
            grabFocus();
        }
        
        center();
    }
    
    public void showAtSize(double percentOfBrowserSize) {
        Page page = Page.getCurrent();

        setWindowMode(WindowMode.NORMAL);

        int pageHeight = page.getBrowserWindowHeight();
        int pageWidth = page.getBrowserWindowWidth();

        setHeight((int) (pageHeight * percentOfBrowserSize), Unit.PIXELS);
        setWidth((int) (pageWidth * percentOfBrowserSize), Unit.PIXELS);

        show();       
       
    }

    @Override
    public void bringToFront() {
        if (isAttached()) {
           super.bringToFront();
        }
    }
    
    public class CloseButtonListener implements ClickListener {

        private static final long serialVersionUID = 1L;

        @Override
        public void buttonClick(ClickEvent event) {
            if (onClose()) {
                close();
            }
        }

    }

}
