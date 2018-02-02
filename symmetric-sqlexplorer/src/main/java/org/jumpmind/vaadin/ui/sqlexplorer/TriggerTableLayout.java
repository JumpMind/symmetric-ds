package org.jumpmind.vaadin.ui.sqlexplorer;

import java.util.Map;

import org.jumpmind.db.model.Trigger;
import org.jumpmind.vaadin.ui.common.ReadOnlyTextAreaDialog;
import org.jumpmind.vaadin.ui.sqlexplorer.TriggerInfoPanel.Refresher;

import com.vaadin.v7.data.Property;
import com.vaadin.v7.event.ItemClickEvent;
import com.vaadin.v7.event.ItemClickEvent.ItemClickListener;
import com.vaadin.server.FontAwesome;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.v7.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.v7.ui.Grid;
import com.vaadin.v7.ui.Grid.SelectionMode;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.v7.ui.Label;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;

public class TriggerTableLayout extends VerticalLayout{

	private static final long serialVersionUID = 1L;
    
	private Trigger trigger;
	
	private Grid grid;
	
	private Settings settings;
	
	private Refresher refresher;
	
	public TriggerTableLayout(Trigger trigger, Settings settings, Refresher refresher) {
		this.trigger = trigger;
		this.settings = settings;
		this.refresher = refresher;
		
		createTabularLayout();
	}
	
	public void createTabularLayout() {
		this.setSizeFull();
		this.setSpacing(false);
		
		HorizontalLayout bar = new HorizontalLayout();
		bar.setWidth(100, Unit.PERCENTAGE);
        bar.setMargin(new MarginInfo(false, true, false, true));

        HorizontalLayout leftBar = new HorizontalLayout();
        leftBar.setSpacing(true);
        final Label label = new Label(trigger.getFullyQualifiedName(), ContentMode.HTML);
        leftBar.addComponent(label);
        
        bar.addComponent(leftBar);
        bar.setComponentAlignment(leftBar, Alignment.MIDDLE_LEFT);
        bar.setExpandRatio(leftBar, 1);
        
        MenuBar rightBar = new MenuBar();
        rightBar.addStyleName(ValoTheme.MENUBAR_BORDERLESS);
        rightBar.addStyleName(ValoTheme.MENUBAR_SMALL);

        MenuItem refreshButton = rightBar.addItem("", new Command() {
            private static final long serialVersionUID = 1L;

            @Override
            public void menuSelected(MenuItem selectedItem) {
                refresher.refresh();
            }
        });
        refreshButton.setIcon(FontAwesome.REFRESH);

        bar.addComponent(rightBar);
        bar.setComponentAlignment(rightBar, Alignment.MIDDLE_RIGHT);
        
        this.addComponent(bar);
        
        grid = fillGrid(settings);
        grid.setSizeFull();
        
        grid.addItemClickListener(new ItemClickListener() {

            private static final long serialVersionUID = 1L;

            @Override
            public void itemClick(ItemClickEvent event) {
                MouseButton button = event.getButton();
                if (button == MouseButton.LEFT) {
                    Object object = event.getPropertyId();
                    if (object != null && !object.toString().equals("")) {
                        if (event.isDoubleClick()) {

                            Object prop = event.getPropertyId();
                            String header = grid.getColumn(prop).getHeaderCaption();
                            Property<?> p = event.getItem().getItemProperty(prop);
                            if (p != null) {
                                String data = String.valueOf(p.getValue());
                                ReadOnlyTextAreaDialog.show(header, data, false);
                            }

                        } else {
                            Object row = event.getItemId();
                            if (!grid.getSelectedRows().contains(row)) {
                                grid.deselectAll();
                                grid.select(row);
                            } else {
                                grid.deselect(row);
                            }
                        }
                    }
                }
            }
        });

        this.addComponent(grid);
        this.setExpandRatio(grid, 1);
	}
	
	private Grid fillGrid(Settings settings) {
		Grid grid = new Grid();
        grid.setSelectionMode(SelectionMode.MULTI);
        grid.setColumnReorderingAllowed(false);
		
        Map<String, Object> metaData = trigger.getMetaData();  
        grid.addColumn("Property", String.class).setHeaderCaption("Property").setHidable(false);
        grid.addColumn("Value", String.class).setHeaderCaption("Value").setHidable(false);
        
        for (String key : metaData.keySet()) {
        	Object[] row = new Object[2];
        	row[0] = key;
        	row[1] = String.valueOf(metaData.get(key));
        	grid.addRow(row);
        }
        
        grid.getColumn("Property").setWidth(250);
        
		return grid;
	}    
    
}
