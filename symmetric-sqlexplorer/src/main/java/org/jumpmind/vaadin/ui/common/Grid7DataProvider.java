package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;

import com.vaadin.v7.ui.Grid;
import com.vaadin.v7.ui.Grid.Column;

public class Grid7DataProvider implements IDataProvider {
    
    private Grid grid;
    
    public Grid7DataProvider(Grid grid) {
        this.grid = grid;
    }

    @Override
    public Collection<?> getRowItems() {
        return grid.getContainerDataSource().getItemIds();
    }

    @Override
    public List<Column> getColumns() {
        return grid.getColumns();
    }

    @Override
    public Object getCellValue(Object item, Object column) {
        if(column instanceof Column) {
            Object propId = ((Column)column).getPropertyId();
            return grid.getContainerDataSource().getContainerProperty(item, propId).getValue();
        }
        return null;
    }

    @Override
    public boolean isHeaderVisible() {
        return grid.isHeaderVisible();
    }

    @Override
    public String getHeaderValue(Object column) {
        Object propId = ((Column)column).getPropertyId();
        return grid.getDefaultHeaderRow().getCell(propId).getText();
    }
    
}