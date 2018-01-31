package org.jumpmind.vaadin.ui.sqlexplorer;

import org.jumpmind.vaadin.ui.common.NotifyDialog;

import com.vaadin.ui.Component;
import com.vaadin.ui.Notification.Type;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.themes.ValoTheme;

public class SqlExplorerTabPanel extends TabSheet {
	
	private static final long serialVersionUID = 1L;
	
	public SqlExplorerTabPanel() {
		super();
		
		setSizeFull();
        addStyleName(ValoTheme.TABSHEET_FRAMED);
        addStyleName(ValoTheme.TABSHEET_COMPACT_TABBAR);
        addStyleName(ValoTheme.TABSHEET_PADDED_TABBAR);
        
        setCloseHandler(new CloseHandler() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void onTabClose(TabSheet tabsheet, Component tabContent) {
				if (tabContent instanceof QueryPanel && ((QueryPanel) tabContent).commitButtonValue) {
					NotifyDialog.show("Cannot Close Tab",
							"You must commit or rollback queries before closing this tab.",
							null, Type.WARNING_MESSAGE);
				} else {
					tabsheet.removeComponent(tabContent);
				}
			}
        });
	}
	
	

}
