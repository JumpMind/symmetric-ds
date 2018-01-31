package org.jumpmind.vaadin.ui.sqlexplorer;

import com.vaadin.server.Resource;
import com.vaadin.ui.MenuBar.Command;

public interface IDbMenuItem {

	public String getCaption();
	
	public Resource getIcon();
	
	public Command getCommand();
	
}
