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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.Serializable;

import com.vaadin.event.ShortcutAction.KeyCode;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.Button.ClickEvent;
import com.vaadin.flow.component.button.Button.ClickListener;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.UI;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.themes.ValoTheme;

public class ConfirmDialog extends Window {

    private static final long serialVersionUID = 1L;

    public ConfirmDialog(String caption, String text, final IConfirmListener confirmListener) {
        setCaption(caption);
        setModal(true);
        setResizable(true);
        setWidth(400, Unit.PIXELS);
        setHeight(300, Unit.PIXELS);
        setClosable(false);

        VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();
        layout.setSpacing(true);
        layout.setMargin(true);
        setContent(layout);

        if (isNotBlank(text)) {
            TextArea textLabel = new TextArea();
            textLabel.setSizeFull();
            textLabel.setStyleName(ValoTheme.TEXTAREA_BORDERLESS);
            textLabel.setValue(text);
            textLabel.setReadOnly(true);
            layout.add(textLabel);
            layout.setExpandRatio(textLabel, 1);
        }

        HorizontalLayout buttonLayout = new HorizontalLayout();
        buttonLayout.setStyleName(ValoTheme.WINDOW_BOTTOM_TOOLBAR);
        buttonLayout.setSpacing(true);
        buttonLayout.setWidth(100, Unit.PERCENTAGE);

        Label spacer = new Label(" ");
        buttonLayout.add(spacer);
        buttonLayout.setExpandRatio(spacer, 1);

        Button cancelButton = new Button("Cancel");
        cancelButton.setClickShortcut(KeyCode.ESCAPE);
        cancelButton.addClickListener(new ClickListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event) {
                UI.getCurrent().removeWindow(ConfirmDialog.this);
            }
        });
        buttonLayout.add(cancelButton);

        Button okButton = new Button("Ok");
        okButton.setStyleName(ValoTheme.BUTTON_PRIMARY);
        okButton.setClickShortcut(KeyCode.ENTER);
        okButton.addClickListener(new ClickListener() {
            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event) {
                if (confirmListener.onOk()) {
                    UI.getCurrent().removeWindow(ConfirmDialog.this);
                }
            }
        });
        buttonLayout.add(okButton);

        layout.add(buttonLayout);
        
        okButton.focus();

    }

    public static void show(String caption, String text, IConfirmListener listener) {
        ConfirmDialog dialog = new ConfirmDialog(caption, text, listener);
        UI.getCurrent().addWindow(dialog);
    }

    public static interface IConfirmListener extends Serializable {
        public boolean onOk();
    }

}
