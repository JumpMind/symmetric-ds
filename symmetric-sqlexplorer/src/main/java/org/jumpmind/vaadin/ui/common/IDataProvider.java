package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;

public interface IDataProvider<T> {
   
    public Collection<?> getRowItems();
    
    public List<?> getColumns();
    
    public Object getCellValue(T item, Object column);
    
    public String getKeyValue(Object column);
    
}