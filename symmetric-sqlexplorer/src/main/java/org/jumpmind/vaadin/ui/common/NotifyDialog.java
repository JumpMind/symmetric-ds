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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.vaadin.server.FontAwesome;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.v7.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.v7.ui.Label;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.UI;

public class NotifyDialog extends ResizableWindow {

    private static final long serialVersionUID = 1L;

    boolean detailsMode = false;

    public NotifyDialog(String text, Throwable ex) {
        this("Error", text, ex, Type.ERROR_MESSAGE);
    }

    public NotifyDialog(String caption, String text, final Throwable ex, Type type) {
        super(caption);
        setWidth(400, Unit.PIXELS);
        setHeight(300, Unit.PIXELS);

        final HorizontalLayout messageArea = new HorizontalLayout();
        messageArea.addStyleName("v-scrollable");
        messageArea.setMargin(true);
        messageArea.setSpacing(true);
        messageArea.setSizeFull();
        
        text = isNotBlank(text) ? text : (ex != null ? ex.getMessage()
                : "");
        if (type == Type.ERROR_MESSAGE) {
            setIcon(FontAwesome.BAN);
        }
        
        final String message = text;
        
        final Label textLabel = new Label(message, ContentMode.HTML);
        messageArea.addComponent(textLabel);
        messageArea.setExpandRatio(textLabel, 1);
        
        content.addComponent(messageArea);
        content.setExpandRatio(messageArea, 1);

        final Button detailsButton = new Button("Details");
        detailsButton.setVisible(ex != null);
        detailsButton.addClickListener(new ClickListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void buttonClick(ClickEvent event) {
                detailsMode = !detailsMode;
                if (detailsMode) {
                    String msg = "<pre>" + ExceptionUtils.getStackTrace(ex).trim() + "</pre>";
                    msg = msg.replace("\t", "    ");
                    textLabel.setValue(msg);
                    detailsButton.setCaption("Message");
                    messageArea.setMargin(new MarginInfo(false, false, false, true));
                    setHeight(600, Unit.PIXELS);
                    setWidth(1000, Unit.PIXELS);
                    setPosition(getPositionX()-300, getPositionY()-150);
                } else {
                    textLabel.setValue(message);
                    detailsButton.setCaption("Details");
                    messageArea.setMargin(true);
                    setWidth(400, Unit.PIXELS);
                    setHeight(300, Unit.PIXELS);
                    setPosition(getPositionX()+300, getPositionY()+150);
                }
            }
        });

        content.addComponent(buildButtonFooter(detailsButton, buildCloseButton()));

    }

    public static void show(String caption, String text, Throwable throwable, Type type) {
        UI.getCurrent().addWindow(new NotifyDialog(caption, text, throwable, type));
    }

}
