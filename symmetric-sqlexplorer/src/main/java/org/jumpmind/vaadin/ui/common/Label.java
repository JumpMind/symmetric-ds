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
import com.vaadin.flow.component.Html;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;

public class Label extends Span {
    
    private static final long serialVersionUID = 1L;
    
    private Html text;
    
    private Component icon;

    public Label() {
        super();
    }
    
    public Label(String text) {
        this.text = new Html("<div>"+text+"</div>");
        getElement().getStyle().set("display", "flex");
        getElement().getStyle().set("margin", "auto");
        add(this.text);
    }
    
    public Label(VaadinIcon icon, String text) {
        this(new Icon(icon), text);
        
    }
    
    public Label(String text, VaadinIcon icon) {
        this(text, new Icon(icon));
    }
    
    public Label(Icon icon, String text) {
        this(text);
        this.icon = icon;
        this.icon.getElement().getStyle().set("float", "right");
        configureIcon(this.icon);
    }
    
    public Label(String text, Icon icon) {
        this(text);
        this.icon = icon;
        this.icon.getElement().getStyle().set("float", "left");
        configureIcon(this.icon);
    }
    
    public Label(Component component) {
        setText(component.getElement().getOuterHTML());
    }
    
    public String getText() {
        return getElement().getProperty("innerHTML");
    }
    
    public void setText(String text) {
        if (this.text != null) {
            remove(this.text);
        }
        this.text = new Html("<div>"+text+"</div>");
        add(this.text);
    }

    public void setRightIcon(VaadinIcon icon) {
        setRightIcon(new Icon(icon));
    }
    
    public void setLeftIcon(VaadinIcon icon) {
        setLeftIcon(new Icon(icon));
    }
    
    public void setLeftIcon(Component icon) {
        if (this.icon != null) {
            remove(this.icon);
        }
        icon.getElement().getStyle().set("align-self", "flex-start");
        this.icon = icon;
        configureIcon(this.icon);
    }

    public void setRightIcon(Component icon) {
        if (this.icon != null) {
            remove(this.icon);
        }
        icon.getElement().getStyle().set("align-self", "flex-end");
        this.icon = icon;
        configureIcon(this.icon);
    }
    
    private void configureIcon(Component icon) {
        add(icon);
    }

}
