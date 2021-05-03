package org.jumpmind.vaadin.ui.sqlexplorer;

import com.vaadin.flow.component.ClickEvent;
import com.vaadin.flow.component.ComponentEventListener;
import com.vaadin.flow.component.contextmenu.MenuItem;
import com.vaadin.flow.component.icon.Icon;

public interface IDbMenuItem {

    public String getCaption();
    
    public Icon getIcon();
    
    public ComponentEventListener<ClickEvent<MenuItem>> getListener();
    
}
