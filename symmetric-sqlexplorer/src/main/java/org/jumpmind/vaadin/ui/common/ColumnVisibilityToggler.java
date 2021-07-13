package org.jumpmind.vaadin.ui.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.contextmenu.ContextMenu;
import com.vaadin.flow.component.grid.Grid.Column;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;

public class ColumnVisibilityToggler extends Button {
    
    private ContextMenu menu;
    
    private Map<Column<?>, Checkbox> columnMap;
    
    private static final long serialVersionUID = 1L;
    
    public ColumnVisibilityToggler() {
        super();
        menu = new ContextMenu(this);
        columnMap = new HashMap<Column<?>, Checkbox>();
        
        menu.setOpenOnClick(true);
        menu.addOpenedChangeListener(event -> {
            if (event.isOpened()) {
                for (Column<?> column : columnMap.keySet()) {
                    columnMap.get(column).setValue(column.isVisible());
                }
            }
        });
        
        setIcon(new Icon(VaadinIcon.COG));
        getElement().setAttribute("title", "Hide/show columns");
    }
    
    public Column<?> addColumn(Column<?> column, String header) {
        if (!columnMap.containsKey(column)) {
            Checkbox checkbox = new Checkbox(header);
            checkbox.addValueChangeListener(event -> column.setVisible(event.getValue()));
            columnMap.put(column, checkbox);
            rebuildMenuItems();
        }
        
        return column;
    }
    
    public boolean isEmpty() {
        return columnMap.isEmpty();
    }
    
    private void rebuildMenuItems() {
        menu.removeAll();
        
        List<Checkbox> checkboxes = columnMap.values().stream().collect(Collectors.toList());
        checkboxes.sort((box0, box1) -> box0.getLabel().compareTo(box1.getLabel()));
        for (Checkbox checkbox : checkboxes) {
            menu.add(checkbox);
        }
    }

}
