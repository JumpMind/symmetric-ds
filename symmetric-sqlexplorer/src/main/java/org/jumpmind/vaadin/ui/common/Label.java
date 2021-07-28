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

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;

public class Label extends Span {
    
    private static final long serialVersionUID = 1L;
    
    private String text;
    
    private Component leftIcon;
    
    private Component rightIcon;

    public Label() {
        super();
    }
    
    public Label(String text) {
        setText(text);
    }
    
    public Label(VaadinIcon icon, String text) {
        setText(text);
        setLeftIcon(icon);
    }
    
    public Label(String text, VaadinIcon icon) {
        setText(text);
        setRightIcon(icon);
    }
    
    public Label(Icon icon, String text) {
        setText(text);
        setLeftIcon(icon);
    }
    
    public Label(String text, Icon icon) {
        setText(text);
        setRightIcon(icon);
    }
    
    public Label(Component component) {
        setText(component.getElement().getOuterHTML());
    }
    
    public void setLeftIcon(VaadinIcon icon) {
        leftIcon = new Icon(icon);
        configureIcon(leftIcon);
        updateLabel();
    }
    
    public void setRightIcon(VaadinIcon icon) {
        rightIcon = new Icon(icon);
        configureIcon(rightIcon);
        updateLabel();
    }
    
    public void setLeftIcon(Component icon) {
        leftIcon = icon;
        configureIcon(leftIcon);
        updateLabel();
    }
    
    public void setRightIcon(Component icon) {
        rightIcon = icon;
        configureIcon(rightIcon);
        updateLabel();
    }
    
    @Override
    public String getText() {
        return getElement().getProperty("innerHTML");
    }
    
    @Override
    public void setText(String text) {
        this.text = text;
        updateLabel();
    }
    
    @Override
    public void removeAll() {
        super.removeAll();
        text = null;
        leftIcon = null;
        rightIcon = null;
    }
    
    private void updateLabel() {
        getElement().setProperty("innerHTML", (leftIcon != null ? leftIcon.getElement().getOuterHTML() + " " : "")
                + (text != null ? text : "") + (rightIcon != null ? " " + rightIcon.getElement().getOuterHTML() : ""));
    }
    
    private void configureIcon(Component icon) {
        icon.getElement().getStyle().set("margin-top", "-4px");
        if (icon != null && icon instanceof Icon) {
            ((Icon) icon).setSize("1em");
        }
    }

}
