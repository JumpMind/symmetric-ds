package org.jumpmind.persist;

import java.util.List;
import java.util.Map;

public interface IPersistenceManager {
    
    public <T> int count(Class<T> clazz, Map<String, Object> conditions);
    
    public <T> int count(Class<T> clazz, String catalogName,
            String schemaName, String tableName, Map<String, Object> conditions);
    
    public int count(String catalogName, String schemaName, String tableName);
    
    public <T> T map(Map<String, Object> row, Class<T> clazz, String catalogName, String schemaName, String tableName);

    public void refresh(Object object, String catalogName, String schemaName, String tableName);

    public <T> List<T> find(Class<T> clazz);

    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions);

    public <T> List<T> find(Class<T> clazz, String catalogName, String schemaName, String tableName);

    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions, String catalogName,
            String schemaName, String tableName);

    public boolean save(Object object, String catalogName, String schemaName, String tableName);

    public boolean save(Object object);

    public boolean delete(Object object, String catalogName, String schemaName, String tableName);

    public boolean delete(Object object);

    public void insert(Object object, String catalogName, String schemaName, String tableName);

    public int update(Object object, String catalogName, String schemaName, String tableName);

}
