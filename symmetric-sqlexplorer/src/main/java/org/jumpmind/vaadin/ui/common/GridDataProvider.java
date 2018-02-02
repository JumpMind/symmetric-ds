package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.vaadin.data.provider.Query;
import com.vaadin.ui.Grid;
import com.vaadin.ui.Grid.Column;

public class GridDataProvider implements IDataProvider{

    private Grid<?> grid;
    
    public GridDataProvider(Grid<?> grid) {
        this.grid = grid;
    }
    
    @Override
    public Collection<?> getRowItems() {
        return (Collection<?>) grid.getDataProvider().fetch(new Query<>()).collect(Collectors.toList());
    }

    @Override
    public List<?> getColumns() {
        return grid.getColumns();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getCellValue(Object item, Object column) {
       if(column instanceof Column) {
           grid.getColumns().stream().map(Column::getId).collect(Collectors.toList());
           return ((Column<Object, ?>) column).getValueProvider().apply(item);
       }
       return null;
    }

    @Override
    public String getHeaderValue(Object column) {
        return grid.getDefaultHeaderRow().getCell((Column<?,?>)column).getText();
    }

    @Override
    public boolean isHeaderVisible() {
        return grid.isHeaderVisible();
    }
    
}