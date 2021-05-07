package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vaadin.flow.data.provider.Query;
import com.vaadin.flow.function.ValueProvider;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.Grid.Column;

public class GridDataProvider<T> implements IDataProvider<T> {

    private Grid<T> grid;
    
    private Map<Column<T>, ValueProvider<T, Object>> valueProviderMap;
    
    public GridDataProvider(Grid<T> grid, Map<Column<T>, ValueProvider<T, Object>> valueProviderMap) {
        this.grid = grid;
        this.valueProviderMap = valueProviderMap;
    }
    
    @Override
    public Collection<?> getRowItems() {
        return (Collection<?>) grid.getDataProvider().fetch(new Query<>()).collect(Collectors.toList());
    }

    @Override
    public List<?> getColumns() {
        return grid.getColumns();
    }

    @Override
    public Object getCellValue(T item, Object column) {
       if (column instanceof Column) {
           return valueProviderMap.get(column).apply(item);
       }
       return null;
    }

    @Override
    public String getKeyValue(Object column) {
        return ((Column<?>) column).getKey();
    }
    
}