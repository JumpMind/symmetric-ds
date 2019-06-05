package org.jumpmind.db.sql;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.SuppressPropertiesBeanIntrospector;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.persist.AbstractPersistenceManager;

public class SqlPersistenceManager extends AbstractPersistenceManager {

    IDatabasePlatform databasePlatform;
    
    // [BEANUTILS-463]
    // Added new SuppressPropertiesBeanIntrospector class to deal with a potential
    // class loader vulnerability.
    private final BeanUtilsBean BEAN_UTILS = new BeanUtilsBean();

    public SqlPersistenceManager(IDatabasePlatform databasePlatform) {
        this.databasePlatform = databasePlatform;
        BEAN_UTILS.getPropertyUtils().addBeanIntrospector(
                SuppressPropertiesBeanIntrospector.SUPPRESS_CLASS);        
    }
    
    @Override
    public <T> T map(Map<String, Object> row, Class<T> clazz, String catalogName, String schemaName, String tableName) {
        try {
            T object = clazz.getDeclaredConstructor().newInstance();
            Table table = findTable(catalogName, schemaName, tableName);
            LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);
            Set<String> propertyNames = objectToTableMapping.keySet();
            for (String propertyName : propertyNames) {
                Object value = row.get(objectToTableMapping.get(propertyName).getName());
                BEAN_UTILS.copyProperty(object, propertyName, value);
            }
            return object;
        } catch (Exception e) {
            throw toRuntimeException(e);
        }
    }

    /**
     * @return true if the object was created, false if the object was updated
     */
    @Override
    public boolean save(Object object) {
        return save(object, null, null, camelCaseToUnderScores(object.getClass().getSimpleName()));
    }

    /**
     * @return true if the object was created, false if the object was updated
     */
    @Override
    public boolean save(Object object, String catalogName, String schemaName, String tableName) {
        if (update(object, catalogName, schemaName, tableName) == 0) {
            insert(object, catalogName, schemaName, tableName);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int update(Object object, String catalogName, String schemaName, String tableName) {
        return excecuteDml(DmlType.UPDATE, object, catalogName, schemaName, tableName);
    }

    @Override
    public void insert(Object object, String catalogName, String schemaName, String tableName) {
        excecuteDml(DmlType.INSERT, object, catalogName, schemaName, tableName);
    }

    @Override
    public boolean delete(Object object) {
        return delete(object, null, null, camelCaseToUnderScores(object.getClass().getSimpleName()));
    }

    @Override
    public boolean delete(Object object, String catalogName, String schemaName, String tableName) {
        return excecuteDml(DmlType.DELETE, object, catalogName, schemaName, tableName) > 0;
    }

    @Override
    public <T> List<T> find(Class<T> clazz) {
        return find(clazz, null, null, camelCaseToUnderScores(clazz.getSimpleName()));
    }

    @Override
    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions) {
        return find(clazz, conditions, null, null, camelCaseToUnderScores(clazz.getSimpleName()));
    }
    
    @Override
    public <T> int count(Class<T> clazz, Map<String, Object> conditions) {
        return count(clazz, null,  null, camelCaseToUnderScores(clazz.getSimpleName()), conditions);
    }
    
    @Override
    public <T> int count(Class<T> clazz, String catalogName, String schemaName, String tableName, Map<String, Object> conditions) {
        if (conditions == null || conditions.size() == 0) {
            return count(catalogName, schemaName, tableName);
        } else {
            try {
                Table table = findTable(catalogName, schemaName, tableName);

                T object = clazz.getDeclaredConstructor().newInstance();

                LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);
                LinkedHashMap<String, Object> objectValuesByColumnName = new LinkedHashMap<String, Object>();

                Column[] keys = new Column[conditions.size()];

                Set<String> keyPropertyNames = conditions.keySet();
                boolean[] nullKeyValues = new boolean[conditions.size()];
                int index = 0;
                for (String propertyName : keyPropertyNames) {
                    Column column = objectToTableMapping.get(propertyName);
                    if (column != null) {
                        keys[index] = column;
                        nullKeyValues[index] = conditions.get(propertyName) == null;
                        objectValuesByColumnName
                                .put(column.getName(), conditions.get(propertyName));
                        index++;
                    } else {
                        throw new IllegalStateException(
                                "Could not find a database column that maps to the " + propertyName
                                        + " property on " + clazz.getName()
                                        + ".  Make sure the property is defined on the class and "
                                        + "the matching column is defined in the database table");
                    }
                }

                DmlStatement statement = databasePlatform.createDmlStatement(DmlType.COUNT,
                        table.getCatalog(), table.getSchema(), table.getName(), keys, keys,
                        nullKeyValues, null);
                String sql = statement.getSql();
                Object[] values = statement.getValueArray(objectValuesByColumnName);

                return databasePlatform.getSqlTemplate().queryForInt(sql, values);
            } catch (Exception e) {
                throw toRuntimeException(e);
            }
        }

    }
    
    @Override
    public int count(String catalogName, String schemaName, String tableName) {
        try {
            Table table = findTable(catalogName, schemaName, tableName);
            DmlStatement statement = databasePlatform.createDmlStatement(DmlType.COUNT,
                    table.getCatalog(), table.getSchema(), table.getName(), null, null, null,
                    null);            
            String sql = statement.getSql();
            return databasePlatform.getSqlTemplate().queryForInt(sql);
        } catch (Exception e) {
            throw toRuntimeException(e);
        }
    }    
    
    public <T> List<T> find(Class<T> clazz, Map<String, Object> conditions, String catalogName,
            String schemaName, String tableName) {
        if (conditions == null || conditions.size() == 0) {
            return find(clazz, catalogName, schemaName, tableName);
        } else {
            try {
                Table table = findTable(catalogName, schemaName, tableName);

                T object = clazz.getDeclaredConstructor().newInstance();

                LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);
                LinkedHashMap<String, Object> objectValuesByColumnName = new LinkedHashMap<String, Object>();

                Column[] keys = new Column[conditions.size()];

                Set<String> keyPropertyNames = conditions.keySet();
                boolean[] nullKeyValues = new boolean[conditions.size()];
                int index = 0;
                for (String propertyName : keyPropertyNames) {
                    Column column = objectToTableMapping.get(propertyName);
                    if (column != null) {
                        keys[index] = column;
                        nullKeyValues[index] = conditions.get(propertyName) == null;
                        objectValuesByColumnName
                                .put(column.getName(), conditions.get(propertyName));
                        index++;
                    } else {
                        throw new IllegalStateException(
                                "Could not find a database column that maps to the " + propertyName
                                        + " property on " + clazz.getName()
                                        + ".  Make sure the property is defined on the class and "
                                        + "the matching column is defined in the database table");
                    }
                }

                Column[] columns = objectToTableMapping.values().toArray(
                        new Column[objectToTableMapping.size()]);

                DmlStatement statement = databasePlatform.createDmlStatement(DmlType.SELECT,
                        table.getCatalog(), table.getSchema(), table.getName(), keys, columns,
                        nullKeyValues, null);
                String sql = statement.getSql();
                Object[] values = statement.getValueArray(objectValuesByColumnName);
                int[] types = statement.getTypes();

                List<Row> rows = databasePlatform.getSqlTemplate().query(sql, values, types);
                List<T> objects = new ArrayList<T>();

                for (Row row : rows) {
                    object = clazz.getDeclaredConstructor().newInstance();
                    Set<String> propertyNames = objectToTableMapping.keySet();
                    for (String propertyName : propertyNames) {
                        Object value = row.get(objectToTableMapping.get(propertyName).getName());
                        BEAN_UTILS.copyProperty(object, propertyName, value);
                    }
                    objects.add(object);
                }

                return objects;
            } catch (Exception e) {
                throw toRuntimeException(e);
            }
        }

    }

    @Override
    public <T> List<T> find(Class<T> clazz, String catalogName, String schemaName, String tableName) {
        try {
            Table table = findTable(catalogName, schemaName, tableName);

            T object = clazz.getDeclaredConstructor().newInstance();

            LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);

            Column[] columns = objectToTableMapping.values().toArray(
                    new Column[objectToTableMapping.size()]);

            DmlStatement statement = databasePlatform.createDmlStatement(DmlType.SELECT_ALL,
                    table.getCatalog(), table.getSchema(), table.getName(), null, columns, null,
                    null);
            String sql = statement.getSql();

            List<Row> rows = databasePlatform.getSqlTemplate().query(sql);
            List<T> objects = new ArrayList<T>();

            for (Row row : rows) {
                object = clazz.getDeclaredConstructor().newInstance();
                Set<String> propertyNames = objectToTableMapping.keySet();
                for (String propertyName : propertyNames) {
                    Object value = row.get(objectToTableMapping.get(propertyName).getName());
                    BEAN_UTILS.copyProperty(object, propertyName, value);
                }
                objects.add(object);
            }

            return objects;
        } catch (Exception e) {
            throw toRuntimeException(e);
        }
    }

    @Override
    public void refresh(Object object, String catalogName, String schemaName, String tableName) {
        try {
            Table table = findTable(catalogName, schemaName, tableName);

            LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);
            LinkedHashMap<String, Object> objectValuesByColumnName = getObjectValuesByColumnName(
                    object, objectToTableMapping);

            Column[] columns = objectToTableMapping.values().toArray(
                    new Column[objectToTableMapping.size()]);
            List<Column> keys = new ArrayList<Column>(1);
            for (Column column : columns) {
                if (column.isPrimaryKey()) {
                    keys.add(column);
                }
            }

            DmlStatement statement = databasePlatform.createDmlStatement(DmlType.SELECT,
                    table.getCatalog(), table.getSchema(), table.getName(),
                    keys.toArray(new Column[keys.size()]), columns, null, null);
            String sql = statement.getSql();
            Object[] values = statement.getValueArray(objectValuesByColumnName);

            Row row = databasePlatform.getSqlTemplate().queryForRow(sql, values);

            if (row != null) {
                Set<String> propertyNames = objectToTableMapping.keySet();
                for (String propertyName : propertyNames) {
                    Object value = row.get(objectToTableMapping.get(propertyName).getName());
                    BEAN_UTILS.copyProperty(object, propertyName, value);
                }
            }
        } catch (Exception e) {
            throw toRuntimeException(e);
        }

    }

    protected int excecuteDml(DmlType type, Object object, String catalogName, String schemaName,
            String tableName) {
        Table table = findTable(catalogName, schemaName, tableName);

        LinkedHashMap<String, Column> objectToTableMapping = mapObjectToTable(object, table);
        LinkedHashMap<String, Object> objectValuesByColumnName = getObjectValuesByColumnName(
                object, objectToTableMapping);

        Column[] columns = objectToTableMapping.values().toArray(
                new Column[objectToTableMapping.size()]);
        List<Column> keys = new ArrayList<Column>(1);
        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                keys.add(column);
            }
        }

        boolean[] nullKeyValues = new boolean[keys.size()];
        int i = 0;
        for (Column column : keys) {
            nullKeyValues[i++] = objectValuesByColumnName.get(column.getName()) == null;
        }

        DmlStatement statement = databasePlatform.createDmlStatement(type, table.getCatalog(),
                table.getSchema(), table.getName(), keys.toArray(new Column[keys.size()]), columns,
                nullKeyValues, null);
        String sql = statement.getSql();
        Object[] values = statement.getValueArray(objectValuesByColumnName);
        int[] types = statement.getTypes();

        return databasePlatform.getSqlTemplate().update(sql, values, types);

    }

    private Table findTable(String catalogName, String schemaName, String tableName) {
        Table table = databasePlatform.getTableFromCache(catalogName, schemaName, tableName, false);
        if (table == null) {
            throw new SqlException("Could not find table "
                    + Table.getFullyQualifiedTableName(catalogName, schemaName, tableName));
        } else {
            return table;
        }
    }

    protected LinkedHashMap<String, Object> getObjectValuesByColumnName(Object object,
            LinkedHashMap<String, Column> objectToTableMapping) {
        try {
            LinkedHashMap<String, Object> objectValuesByColumnName = new LinkedHashMap<String, Object>();
            Set<String> propertyNames = objectToTableMapping.keySet();
            for (String propertyName : propertyNames) {
                objectValuesByColumnName.put(objectToTableMapping.get(propertyName).getName(),
                        PropertyUtils.getProperty(object, propertyName));
            }
            return objectValuesByColumnName;
        } catch (IllegalAccessException e) {
            throw toRuntimeException(e);
        } catch (InvocationTargetException e) {
            throw toRuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw toRuntimeException(e);
        }
    }

    protected LinkedHashMap<String, Column> mapObjectToTable(Object object, Table table) {
        LinkedHashMap<String, Column> columnNames = new LinkedHashMap<String, Column>();
        PropertyDescriptor[] pds = PropertyUtils.getPropertyDescriptors(object);
        for (int i = 0; i < pds.length; i++) {
            String propName = pds[i].getName();
            Column column = table.getColumnWithName(camelCaseToUnderScores(propName));
            if (column != null) {
                columnNames.put(propName, column);
            }
        }
        return columnNames;
    }

    protected String camelCaseToUnderScores(String camelCaseName) {
        StringBuilder underscoredName = new StringBuilder();
        for (int p = 0; p < camelCaseName.length(); p++) {
            char c = camelCaseName.charAt(p);
            if (p > 0 && Character.isUpperCase(c)) {
                underscoredName.append("_");
            }
            underscoredName.append(Character.toLowerCase(c));

        }
        return underscoredName.toString();
    }

    public void setDatabasePlatform(IDatabasePlatform databasePlatform) {
        this.databasePlatform = databasePlatform;
    }

    public IDatabasePlatform getDatabasePlatform() {
        return databasePlatform;
    }
}
