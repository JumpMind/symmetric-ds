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

import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.textfield.TextArea;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class ConfirmDialog extends Dialog {

    private static final long serialVersionUID = 1L;

    public ConfirmDialog(String caption, String text, final IConfirmListener confirmListener) {
        setModal(true);
        setResizable(true);
        setWidth("400px");
        setHeight("300px");
        setCloseOnEsc(false);
        setCloseOnOutsideClick(false);
        
        if (caption != null) {
            add(new Label(caption + "<hr>"));
        }

        VerticalLayout layout = new VerticalLayout();
        layout.setWidthFull();
        layout.setSpacing(true);
        layout.setMargin(false);
        add(layout);

        if (isNotBlank(text)) {
            TextArea textLabel = new TextArea();
            textLabel.setSizeFull();
            //textLabel.setClassName(ValoTheme.TEXTAREA_BORDERLESS);
            textLabel.setValue(text);
            textLabel.setReadOnly(true);
            layout.addAndExpand(textLabel);
        }
        layout.setHeight("90%");

        HorizontalLayout buttonLayout = new HorizontalLayout();
        //buttonLayout.setClassName(ValoTheme.WINDOW_BOTTOM_TOOLBAR);
        buttonLayout.setSpacing(true);

        buttonLayout.addAndExpand(new Span());

        Button cancelButton = new Button("Cancel");
        cancelButton.addClickShortcut(Key.ESCAPE);
        cancelButton.addClickListener(event -> close());
        buttonLayout.add(cancelButton);

        Button okButton = new Button("Ok");
        okButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        okButton.addClickShortcut(Key.ENTER);
        okButton.addClickListener(event -> {
            if (confirmListener.onOk()) {
                close();
            }
        });
        buttonLayout.add(okButton);

        layout.add(buttonLayout);
        
        okButton.focus();

    }

    public static void show(String caption, String text, IConfirmListener listener) {
        ConfirmDialog dialog = new ConfirmDialog(caption, text, listener);
        dialog.open();
    }

    public static interface IConfirmListener extends Serializable {
        public boolean onOk();
    }

}
