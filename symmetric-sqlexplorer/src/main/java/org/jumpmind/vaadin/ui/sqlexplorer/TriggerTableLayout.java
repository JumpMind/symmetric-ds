package org.jumpmind.vaadin.ui.sqlexplorer;

import java.util.Map;

import org.jumpmind.db.model.Trigger;
import org.jumpmind.vaadin.ui.common.ReadOnlyTextAreaDialog;
import org.jumpmind.vaadin.ui.sqlexplorer.TriggerInfoPanel.Refresher;

import com.vaadin.icons.VaadinIcons;
import com.vaadin.shared.MouseEventDetails.MouseButton;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Grid.SelectionMode;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;

public class TriggerTableLayout extends VerticalLayout{

    private static final long serialVersionUID = 1L;
    
    private Trigger trigger;
    
    private Grid<String> grid;
    
    private Refresher refresher;
    
    public TriggerTableLayout(Trigger trigger, Settings settings, Refresher refresher) {
        this.trigger = trigger;
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
        refreshButton.setIcon(VaadinIcons.REFRESH);

        bar.addComponent(rightBar);
        bar.setComponentAlignment(rightBar, Alignment.MIDDLE_RIGHT);
        
        this.addComponent(bar);
        
        grid = fillGrid();
        grid.setSizeFull();
        
        grid.addItemClickListener(event -> {
            MouseButton button = event.getMouseEventDetails().getButton();
            if (button == MouseButton.LEFT && event.getColumn() != null) {
                if (event.getMouseEventDetails().isDoubleClick()) {
                    String colId = event.getColumn().getId();
                    if (colId.equals("property")) {
                        ReadOnlyTextAreaDialog.show("Property", event.getItem(), false);
                    } else if (colId.equals("value")) {
                        ReadOnlyTextAreaDialog.show("Value", (String) trigger.getMetaData().get(event.getItem()), false);
                    }
                } else {
                    grid.deselectAll();
                    grid.select(event.getItem());
                }
            }
        });

        this.addComponent(grid);
        this.setExpandRatio(grid, 1);
    }
    
    private Grid<String> fillGrid() {
        Grid<String> grid = new Grid<String>();
        grid.setSelectionMode(SelectionMode.MULTI);
        grid.setColumnReorderingAllowed(false);
        
        Map<String, Object> metaData = trigger.getMetaData();
        grid.addColumn(property -> property).setId("property").setCaption("Property").setWidth(250);
        grid.addColumn(property -> String.valueOf(metaData.get(property))).setId("value").setCaption("Value");
        
        grid.setItems(metaData.keySet());
        
        return grid;
    }    
    
}
