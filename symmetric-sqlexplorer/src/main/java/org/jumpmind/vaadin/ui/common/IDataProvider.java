package org.jumpmind.vaadin.ui.common;

import java.util.Collection;
import java.util.List;

public interface IDataProvider {
   
    public Collection<?> getRowItems();
    
    public List<?> getColumns();
    
    public Object getCellValue(Object item, Object column);
    
    public String getHeaderValue(Object column);
    
    public boolean isHeaderVisible();
    
}