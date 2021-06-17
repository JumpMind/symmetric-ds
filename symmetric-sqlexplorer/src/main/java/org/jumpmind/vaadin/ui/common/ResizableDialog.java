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

import com.vaadin.flow.component.ClickEvent;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.ComponentEventListener;
import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.KeyModifier;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.UI;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class ResizableDialog extends Dialog {

    private static final long serialVersionUID = 1L;

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected VerticalLayout content;

    public ResizableDialog() {
        this("");
    }
    
    public ResizableDialog(String caption) {
        //setCaption(caption);
        setModal(true);
        setResizable(true);
        
        content = new VerticalLayout();
        content.setSizeFull();
        content.setMargin(false);
        content.setSpacing(false);
        super.add(content);
        
        UI.getCurrent().addShortcutListener(() -> {
            if (!"100%".equals(getWidth())) {
                setWidth("100%");
                setHeight("100%");
            } else {
                setWidth(null);
                setHeight(null);
            }
        }, Key.KEY_M, KeyModifier.CONTROL);
        
        UI.getCurrent().addShortcutListener(() -> close(), Key.ESCAPE);
    }
    
    protected void add(Component component, int expandRatio) {
        content.add(component);
        content.setFlexGrow(expandRatio, component);
    }
    
    protected void add(Component component) {
        content.add(component);
    }
    
    protected void addComponents(Component... components) {
        for (Component component : components) {
            content.add(component);    
        }        
    }

    protected Button buildCloseButton() {
        Button closeButton = new Button("Close");
        closeButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        closeButton.addClickListener(new CloseButtonListener());
        closeButton.focus();
        return closeButton;
    }
    
    protected HorizontalLayout buildButtonFooter(Component... toTheRightButtons) {
        return buildButtonFooter((Component[])null, toTheRightButtons);
    }

    protected HorizontalLayout buildButtonFooter(Component[] toTheLeftButtons, Component... toTheRightButtons) {
        HorizontalLayout footer = new HorizontalLayout();

        footer.setWidth("100%");
        footer.setSpacing(true);
        //footer.addClassName(ValoTheme.WINDOW_BOTTOM_TOOLBAR);

        if (toTheLeftButtons != null) {
            footer.add(toTheLeftButtons);
        }
        
        Span footerText = new Span("");
        footerText.setSizeUndefined();

        footer.add(footerText);
        footer.expand(footerText);

        if (toTheRightButtons != null) {
            footer.add(toTheRightButtons);
        }


        return footer;
    }
    
    protected boolean onClose() { return true;}

    public void show() {
        open();
        //center();
    }
    
    public void showAtSize(double percentOfBrowserSize) {
        UI.getCurrent().getPage().retrieveExtendedClientDetails(details -> {
            setHeight((details.getWindowInnerHeight() * percentOfBrowserSize) + "px");
            setWidth((details.getWindowInnerWidth() * percentOfBrowserSize) + "px");
        });

        show();       
       
    }

    public void bringToFront() {
        if (getElement().getNode().isAttached()) {
           super.getElement().executeJs("$0._bringOverlayToFront()");
        }
    }
    
    public class CloseButtonListener implements ComponentEventListener<ClickEvent<Button>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void onComponentEvent(ClickEvent<Button> event) {
            if (onClose()) {
                close();
            }
        }

    }

}
